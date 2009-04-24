using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using log4net;
using Rhino.Queues.Exceptions;
using Rhino.Queues.Model;
using Rhino.Queues.Storage;
using Wintellect.Threading.AsyncProgModel;

namespace Rhino.Queues.Protocol
{
    public class Sender : IDisposable
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(Sender));
        
        public event Action SendCompleted;
        public Func<MessageBookmark[]> Success { get; set; }
        public Action<Exception> Failure { get; set; }
        public Action<MessageBookmark[]> Revert { get; set; }
        public Endpoint Destination { get; set; }
        public Message[] Messages { get; set; }

        public Sender()
        {
            Failure = e => { };
            Success = () => null;
            Revert = bookmarks => { };
        }

        public void Send()
        {
            var enumerator = new AsyncEnumerator(
                string.Format("Sending {0} messages to {1}", Messages.Length,
                              Destination)
                );

            logger.DebugFormat("Starting to send {0} messages to {1}", Messages.Length, Destination);
            enumerator.BeginExecute(SendInternal(enumerator), result =>
            {
                try
                {
                    enumerator.EndExecute(result);
                }
                catch (Exception exception)
                {
                    logger.Warn("Failed to send message", exception);
                    Failure(exception);
                }
            });
        }

        private IEnumerator<int> SendInternal(AsyncEnumerator ae)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    try
                    {
                        client.BeginConnect(Destination.Host, Destination.Port,
                                            ae.End(),
                                            null);
                    }
                    catch (Exception exception)
                    {
                        logger.WarnFormat("Failed to connect to {0} because {1}", Destination, exception);
                        Failure(exception);
                        yield break;
                    }

                    yield return 1;

                    try
                    {
                        client.EndConnect(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        logger.WarnFormat("Failed to connect to {0} because {1}", Destination, exception);
                        Failure(exception);
                        yield break;
                    }

                    logger.DebugFormat("Successfully connected to {0}", Destination);

                    using (var stream = client.GetStream())
                    {
                        var buffer = Messages.Serialize();

                        var bufferLenInBytes = BitConverter.GetBytes(buffer.Length);

                        logger.DebugFormat("Writing length of {0} bytes to {1}", buffer.Length, Destination);

                        try
                        {
                            stream.BeginWrite(bufferLenInBytes, 0, bufferLenInBytes.Length, ae.End(), null);
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not write to {0} because {1}", Destination,
                                              exception);
                            Failure(exception);
                            yield break;
                        }
                    
                        yield return 1;

                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not write to {0} because {1}", Destination,
                                              exception);
                            Failure(exception);
                            yield break;
                        }

                        logger.DebugFormat("Writing {0} bytes to {1}", buffer.Length, Destination);

                        try
                        {
                            stream.BeginWrite(buffer, 0, buffer.Length, ae.End(), null);
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not write to {0} because {1}", Destination,
                                            exception);
                            Failure(exception);
                            yield break;
                        }
                    
                        yield return 1;

                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not write to {0} because {1}", Destination,
                                              exception);
                            Failure(exception);
                            yield break;
                        }

                        logger.DebugFormat("Successfully wrote to {0}", Destination);

                        var recieveBuffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                        var readConfirmationEnumerator = new AsyncEnumerator();

                        try
                        {
                            readConfirmationEnumerator.BeginExecute(
                                StreamUtil.ReadBytes(recieveBuffer, stream, readConfirmationEnumerator, "recieve confirmation", false), ae.End());
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not read confirmation from {0} because {1}", Destination,
                                              exception);
                            Failure(exception);
                            yield break;
                        }

                        yield return 1;

                        try
                        {
                            readConfirmationEnumerator.EndExecute(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Could not read confirmation from {0} because {1}", Destination,
                                              exception);
                            Failure(exception);
                            yield break;
                        }

                        var recieveRespone = Encoding.Unicode.GetString(recieveBuffer);
                        if (recieveRespone == ProtocolConstants.QueueDoesNotExists)
                        {
                            logger.WarnFormat(
                                "Response from reciever {0} is that queue does not exists",
                                Destination);
                            Failure(new QueueDoesNotExistsException());
                            yield break;
                        }
                        else if(recieveRespone!=ProtocolConstants.Recieved)
                        {
                            logger.WarnFormat(
                                "Response from reciever {0} is not the expected one, unexpected response was: {1}",
                                Destination, recieveRespone);
                            Failure(null);
                            yield break;
                        }

                        try
                        {
                            stream.BeginWrite(ProtocolConstants.AcknowledgedBuffer, 0,
                                              ProtocolConstants.AcknowledgedBuffer.Length, ae.End(), null);
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Failed to write acknowledgement to reciever {0} because {1}",
                                              Destination, exception);
                            Failure(exception);
                            yield break;
                        }

                        yield return 1;

                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.WarnFormat("Failed to write acknowledgement to reciever {0} because {1}",
                                              Destination, exception);
                            Failure(exception);
                            yield break;
                        }

                        var bookmarks = Success();

                        buffer = new byte[ProtocolConstants.RevertBuffer.Length];
                        var readRevertMessage = new AsyncEnumerator(ae.ToString());
                        bool startingToReadFailed = false;
                        try
                        {
                            readRevertMessage.BeginExecute(
                                StreamUtil.ReadBytes(buffer, stream, readRevertMessage, "revert", true), ae.End());
                        }
                        catch (Exception)
                        {
                            //more or less expected
                            startingToReadFailed = true;
                        }
                        if (startingToReadFailed)
                            yield break;
                        yield return 1;
                        try
                        {
                            readRevertMessage.EndExecute(ae.DequeueAsyncResult());
                            var revert = Encoding.Unicode.GetString(buffer);
                            if (revert == ProtocolConstants.Revert)
                            {
                            	logger.Warn("Got back revert message from receiver, reverting send");
                                Revert(bookmarks);
                            }
                        }
                        catch (Exception)
                        {
                            // expected, there is nothing to do here, the
                            // reciever didn't report anything for us
                        }

                    }
                }
            }
            finally
            {
                var completed = SendCompleted;
                if (completed != null)
                    completed();
            }
        }

        public void Dispose()
        {

        }
    }
}
