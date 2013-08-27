using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using Wintellect.Threading.AsyncProgModel;

namespace LightningQueues.Protocol
{
    public class Receiver : IDisposable
    {
        private readonly IPEndPoint endpointToListenTo;
        private readonly bool enableEndpointPortAutoSelection;
        private readonly Func<Message[], IMessageAcceptance> acceptMessages;
        private readonly ILogger _logger;
        private TcpListener listener;

        public event Action CompletedRecievingMessages;

        public Receiver(IPEndPoint endpointToListenTo, Func<Message[], IMessageAcceptance> acceptMessages, ILogger logger)
            :this(endpointToListenTo, false, acceptMessages, logger)
        { }

        public Receiver(IPEndPoint endpointToListenTo, bool enableEndpointPortAutoSelection, Func<Message[], IMessageAcceptance> acceptMessages, ILogger logger)
        {
            this.endpointToListenTo = endpointToListenTo;
            this.enableEndpointPortAutoSelection = enableEndpointPortAutoSelection;
            this.acceptMessages = acceptMessages;
            _logger = logger;
        }

        public void Start()
        {
            _logger.Debug("Starting to listen on {0}", endpointToListenTo);
            while (endpointToListenTo.Port < 65536)
            {
                try
                {
                    TryStart(endpointToListenTo);
                    _logger.Debug("Now listen on {0}", endpointToListenTo);
                    return;
                }
                catch(SocketException ex)
                {
                    if (enableEndpointPortAutoSelection &&
                        ex.Message == "Only one usage of each socket address (protocol/network address/port) is normally permitted")
                    {
                        endpointToListenTo.Port = SelectAvailablePort();
                        _logger.Debug("Port in use, new enpoint selected: {0}", endpointToListenTo);
                    }
                    else
                        throw;
                }
            }
        }

        private void TryStart(IPEndPoint endpointToListenTo)
        {
            listener = new TcpListener(endpointToListenTo);
            listener.Start();
            listener.BeginAcceptTcpClient(BeginAcceptTcpClientCallback, null);
        }

        private static int SelectAvailablePort()
        {
            const int START_OF_IANA_PRIVATE_PORT_RANGE = 49152;
            var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
            var tcpListeners = ipGlobalProperties.GetActiveTcpListeners();
            var tcpConnections = ipGlobalProperties.GetActiveTcpConnections();

            var allInUseTcpPorts = tcpListeners.Select(tcpl => tcpl.Port)
                .Union(tcpConnections.Select(tcpi => tcpi.LocalEndPoint.Port));

            var orderedListOfPrivateInUseTcpPorts = allInUseTcpPorts
                .Where(p => p >= START_OF_IANA_PRIVATE_PORT_RANGE)
                .OrderBy(p => p);

            var candidatePort = START_OF_IANA_PRIVATE_PORT_RANGE;
            foreach (var usedPort in orderedListOfPrivateInUseTcpPorts)
            {
                if (usedPort != candidatePort) break;
                candidatePort++;
            }
            return candidatePort;
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
			catch (Exception ex)
			{
				_logger.Info("Error on EndAcceptTcpClient {0}", ex);
				StartAcceptingTcpClient();
				return;
			}

            _logger.Debug("Accepting connection from {0}", client.Client.RemoteEndPoint);
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
                    _logger.Info("Failed to recieve message {0}", exception);
                }
            });

			StartAcceptingTcpClient();
		}

		private void StartAcceptingTcpClient()
		{
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
                        _logger.Info("Unable to read length data from {0} {1}", sender, exception);
                        yield break;
                    }

                    yield return 1;

                    try
                    {
                        lenEnumerator.EndExecute(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        _logger.Info("Unable to read length data from {0} {1}", sender, exception);
                        yield break;
                    }

                    var lengthOfDataToRead = BitConverter.ToInt32(lenOfDataToReadBuffer, 0);
                    if (lengthOfDataToRead < 0)
                    {
                        _logger.Info("Got invalid length {0} from sender {1}", lengthOfDataToRead, sender);
                        yield break;
                    }
                    _logger.Debug("Reading {0} bytes from {1}", lengthOfDataToRead, sender);

                    var buffer = new byte[lengthOfDataToRead];

                    var readBufferEnumerator = new AsyncEnumerator(ae.ToString());
                    try
                    {
                        readBufferEnumerator.BeginExecute(
                            StreamUtil.ReadBytes(buffer, stream, readBufferEnumerator, "message data", false), ae.End());
                    }
                    catch (Exception exception)
                    {
                        _logger.Info("Unable to read message data from {0} {1}", sender, exception);
                        yield break;
                    }
                    yield return 1;

                    try
                    {
                        readBufferEnumerator.EndExecute(ae.DequeueAsyncResult());
                    }
                    catch (Exception exception)
                    {
                        _logger.Info("Unable to read message data from {0} {1}", sender, exception);
                        yield break;
                    }

                    Message[] messages = null;
                    try
                    {
                        messages = SerializationExtensions.ToMessages(buffer);
                        _logger.Debug("Deserialized {0} messages from {1}", messages.Length, sender);
                    }
                    catch (Exception exception)
                    {
                        _logger.Info("Failed to deserialize messages from {0} {1}", sender, exception);
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
                            _logger.Info("Unable to send serialization format error to {0} {1}", sender, exception);
                            yield break;
                        }
                        yield return 1;
                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            _logger.Info("Unable to send serialization format error to {0} {1}", sender, exception);
                        }

                        yield break;
                    }

                    IMessageAcceptance acceptance = null;
                    byte[] errorBytes = null;
                    try
                    {
                        acceptance = acceptMessages(messages);
                        _logger.Debug("All messages from {0} were accepted", sender);
                    }
                    catch (QueueDoesNotExistsException)
                    {
                        _logger.Info("Failed to accept messages from {0} because queue does not exists", sender);
                        errorBytes = ProtocolConstants.QueueDoesNoExiststBuffer;
                    }
                    catch (Exception exception)
                    {
                        errorBytes = ProtocolConstants.ProcessingFailureBuffer;
                        _logger.Info("Failed to accept messages from {0} {1}", sender, exception);
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
                            _logger.Info("Unable to send processing failure from {0} {1}" + sender, exception);
                            yield break;
                        }
                        yield return 1;
                        try
                        {
                            stream.EndWrite(ae.DequeueAsyncResult());
                        }
                        catch (Exception exception)
                        {
                            _logger.Info("Unable to send processing failure from {0} {1}", sender, exception);
                        }
                        yield break;
                    }

                    _logger.Debug("Sending reciept notice to {0}", sender);
                    try
                    {
                        stream.BeginWrite(ProtocolConstants.RecievedBuffer, 0, ProtocolConstants.RecievedBuffer.Length,
                                          ae.End(), null);
                    }
                    catch (Exception exception)
                    {
                        _logger.Info("Could not send reciept notice to {0} {1}", sender, exception);
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
                        _logger.Info("Could not send reciept notice to {0} {1}", sender, exception);
                        acceptance.Abort();
                        yield break;
                    }

                    _logger.Debug("Reading acknowledgement about accepting messages to {0}", sender);

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
                        _logger.Info("Error reading acknowledgement from {0} {1}", sender, exception);
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
                        _logger.Info("Error reading acknowledgement from {0} {1}", sender, exception);
                        acceptance.Abort();
                        yield break;
                    }

                    var senderResponse = Encoding.Unicode.GetString(acknowledgementBuffer);
                    if (senderResponse != ProtocolConstants.Acknowledged)
                    {
                        _logger.Info("Sender did not respond with proper acknowledgement, the reply was {0}", senderResponse);
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
                        _logger.Info("Unable to commit messages from {0} {1}", sender, exception);
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
                            _logger.Info("Unable to send revert message to {0} {1}", sender, e);
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
                                _logger.Info("Unable to send revert message to {0} {1}", sender, exception);
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