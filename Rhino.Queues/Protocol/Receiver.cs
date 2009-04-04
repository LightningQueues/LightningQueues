using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using log4net;
using Rhino.Queues.Exceptions;
using Rhino.Queues.Model;
using Wintellect.Threading.AsyncProgModel;

namespace Rhino.Queues.Protocol
{
    public class Receiver : IDisposable
    {
        private readonly IPEndPoint endpointToListenTo;
        private readonly Func<Message[], IMessageAcceptance> acceptMessages;
        private readonly TcpListener listener;
        private readonly ILog logger = LogManager.GetLogger(typeof(Receiver));

        public event Action CompletedRecievingMessages;

        public Receiver(IPEndPoint endpointToListenTo, Func<Message[], IMessageAcceptance> acceptMessages)
        {
            this.endpointToListenTo = endpointToListenTo;
            this.acceptMessages = acceptMessages;
            listener = new TcpListener(endpointToListenTo);
        }

        public void Start()
        {
            logger.DebugFormat("Starting to listen on {0}", endpointToListenTo);
            listener.Start();
            listener.BeginAcceptTcpClient(BeginAcceptTcpClientCallback, null);
        }

        private void BeginAcceptTcpClientCallback(IAsyncResult result)
        {
            TcpClient client;
            try
            {
                client = listener.EndAcceptTcpClient(result);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            logger.DebugFormat("Accepting connection from {0}", client.Client.RemoteEndPoint);
            var enumerator = new AsyncEnumerator(
                "Receiver from " + client.Client.RemoteEndPoint
                );
            enumerator.BeginExecute(ProcessRequest(client, enumerator), ar =>
            {
                try
                {
                    enumerator.EndExecute(ar);
                }
                catch (Exception exception)
                {
                    logger.Warn("Failed to recieve message", exception);
                }
            });

            try
            {
                listener.BeginAcceptTcpClient(BeginAcceptTcpClientCallback, null);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private IEnumerator<int> ProcessRequest(TcpClient client, AsyncEnumerator ae)
        {
            try
            {
                using (client)
                using (var stream = client.GetStream())
                {
                    var sender = client.Client.RemoteEndPoint;

                    var lenOfDataToReadBuffer = new byte[sizeof(int)];

                    var lenEnumerator = new AsyncEnumerator(ae.ToString());
                    try
                    {
                        lenEnumerator.BeginExecute(
                            StreamUtil.ReadBytes(lenOfDataToReadBuffer, stream, lenEnumerator, "length data",false), ae.End());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Unable to read length data from " + sender, exception);
                        yield break;
                    }

                    yield return 1;

                    try
                    {
                        lenEnumerator.EndExecute(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Unable to read length data from " + sender, exception);
                        yield break;
                    }

                    var lengthOfDataToRead = BitConverter.ToInt32(lenOfDataToReadBuffer, 0);
                    if (lengthOfDataToRead < 0)
                    {
                        logger.WarnFormat("Got invalid length {0} from sender {1}", lengthOfDataToRead, sender);
                        yield break;
                    }
                    logger.DebugFormat("Reading {0} bytes from {1}", lengthOfDataToRead, sender);

                    var buffer = new byte[lengthOfDataToRead];

                    var readBufferEnumerator = new AsyncEnumerator(ae.ToString());
                    try
                    {
                        readBufferEnumerator.BeginExecute(
                            StreamUtil.ReadBytes(buffer, stream, readBufferEnumerator, "message data", false), ae.End());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Unable to read message data from " + sender, exception);
                        yield break;
                    }
                    yield return 1;

                    try
                    {
                        readBufferEnumerator.EndExecute(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Unable to read message data from " + sender, exception);
                        yield break;
                    }

                    Message[] messages = null;
                    try
                    {
                        messages = SerializationExtensions.ToMessages(buffer);
                        logger.DebugFormat("Deserialized {0} messages from {1}", messages.Length, sender);
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Failed to deserialize messages from " + sender, exception);
                    }

                    if (messages == null)
                    {
                        try
                        {
                            stream.BeginWrite(ProtocolConstants.SerializationFailureBuffer, 0,
                                              ProtocolConstants.SerializationFailureBuffer.Length, ae.End(), null);
                        }
                        catch (Exception exception)
                        {
                            logger.Warn("Unable to send serialization format error to " + sender, exception);
                            yield break;
                        }
                        yield return 1;
                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.Warn("Unable to send serialization format error to " + sender, exception);
                        }

                        yield break;
                    }

                    IMessageAcceptance acceptance = null;
                    byte[] errorBytes = null;
                    try
                    {
                        acceptance = acceptMessages(messages);
                        logger.DebugFormat("All messages from {0} were accepted", sender);
                    }
                    catch (QueueDoesNotExistsException)
                    {
                        logger.WarnFormat("Failed to accept messages from {0} because queue does not exists", sender);
                        errorBytes = ProtocolConstants.QueueDoesNoExiststBuffer;
                    }
                    catch (Exception exception)
                    {
                        errorBytes = ProtocolConstants.ProcessingFailureBuffer;
                        logger.Warn("Failed to accept messages from " + sender, exception);
                    }

                    if (errorBytes != null)
                    {
                        try
                        {
                            stream.BeginWrite(errorBytes, 0,
                                              errorBytes.Length, ae.End(), null);
                        }
                        catch (Exception exception)
                        {
                            logger.Warn("Unable to send processing failure from " + sender, exception);
                            yield break;
                        }
                        yield return 1;
                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            logger.Warn("Unable to send processing failure from " + sender, exception);
                        }
                        yield break;
                    }

                    logger.DebugFormat("Sending reciept notice to {0}", sender);
                    try
                    {
                        stream.BeginWrite(ProtocolConstants.RecievedBuffer, 0, ProtocolConstants.RecievedBuffer.Length,
                                          ae.End(), null);
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Could not send reciept notice to " + sender, exception);
                        acceptance.Abort();
                        yield break;
                    }
                    yield return 1;

                    try
                    {
                        stream.EndWrite(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Could not send reciept notice to " + sender, exception);
                        acceptance.Abort();
                        yield break;
                    }

                    logger.DebugFormat("Reading acknowledgement about accepting messages to {0}", sender);

                    var acknowledgementBuffer = new byte[ProtocolConstants.AcknowledgedBuffer.Length];

                    var readAcknoweldgement = new AsyncEnumerator(ae.ToString());
                    try
                    {
                        readAcknoweldgement.BeginExecute(
                            StreamUtil.ReadBytes(acknowledgementBuffer, stream, readAcknoweldgement, "acknowledgement", false),
                            ae.End());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Error reading acknowledgement from " + sender, exception);
                        acceptance.Abort();
                        yield break;
                    }
                    yield return 1;
                    try
                    {
                        readAcknoweldgement.EndExecute(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Error reading acknowledgement from " + sender, exception);
                        acceptance.Abort();
                        yield break;
                    }

                    var senderResponse = Encoding.Unicode.GetString(acknowledgementBuffer);
                    if (senderResponse != ProtocolConstants.Acknowledged)
                    {
                        logger.WarnFormat("Sender did not respond with proper acknowledgement, the reply was {0}",
                                          senderResponse);
                        acceptance.Abort();
                    }

                    bool commitSuccessful;
                    try
                    {
                        acceptance.Commit();
                        commitSuccessful = true;
                    }
                    catch (Exception exception)
                    {
                        logger.Warn("Unable to commit messages from " + sender, exception);
                        commitSuccessful = false;
                    }

                    if (commitSuccessful == false)
                    {
                        bool writeSuccessful;
                        try
                        {
                            stream.BeginWrite(ProtocolConstants.RevertBuffer, 0, ProtocolConstants.RevertBuffer.Length,
                                              ae.End(),
                                              null);
                            writeSuccessful = true;
                        }
                        catch (Exception e)
                        {
                            logger.Warn("Unable to send revert message to " + sender, e);
                            writeSuccessful = false;
                        }

                        if (writeSuccessful)
                        {
                            yield return 1;


                            try
                            {
                                stream.EndWrite(ae.DequeueAsyncResult());
                            }
                            catch (Exception exception)
                            {
                                logger.Warn("Unable to send revert message to " + sender, exception);
                            }
                        }
                    }
                }
            }
            finally
            {
                var copy = CompletedRecievingMessages;
                if (copy != null)
                    copy();
            }
        }

        public void Dispose()
        {
            listener.Stop();
        }
    }
}