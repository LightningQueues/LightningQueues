using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Model;

namespace LightningQueues.Protocol
{
    public class Receiver : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IPEndPoint _endpointToListenTo;
        private readonly bool _enableEndpointPortAutoSelection;
        private readonly Func<Message[], IMessageAcceptance> _acceptMessages;
        private TcpListener _listener;
        private bool _disposed;

        public event Action CompletedRecievingMessages;

        public Receiver(IPEndPoint endpointToListenTo, Func<Message[], IMessageAcceptance> acceptMessages, ILogger logger)
            : this(endpointToListenTo, false, acceptMessages, logger)
        {
        }

        public Receiver(IPEndPoint endpointToListenTo, bool enableEndpointPortAutoSelection, Func<Message[], IMessageAcceptance> acceptMessages, ILogger logger)
        {
            _endpointToListenTo = endpointToListenTo;
            _enableEndpointPortAutoSelection = enableEndpointPortAutoSelection;
            _acceptMessages = acceptMessages;
            _logger = logger;
        }

        public void Start()
        {
            _logger.Debug("Starting to listen on {0}", _endpointToListenTo);
            while (_endpointToListenTo.Port < 65536)
            {
                try
                {
                    TryStart(_endpointToListenTo);
                    StartAccepting();
                    _logger.Debug("Now listen on {0}", _endpointToListenTo);
                    return;
                }
                catch (SocketException ex)
                {
                    if (_enableEndpointPortAutoSelection &&
                        ex.Message == "Only one usage of each socket address (protocol/network address/port) is normally permitted")
                    {
                        _endpointToListenTo.Port = new PortFinder().Find();
                        _logger.Debug("Port in use, new enpoint selected: {0}", _endpointToListenTo);
                    }
                    else
                        throw;
                }
            }
        }

        private async void StartAccepting()
        {
            while (!_disposed)
            {
                await AcceptTcpClientAsync();
            }
        }

        private void TryStart(IPEndPoint endpointToListenTo)
        {
            _listener = new TcpListener(endpointToListenTo);
            _listener.Start();
        }

        private async Task AcceptTcpClientAsync()
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync();
                _logger.Debug("Accepting connection from {0}", client.Client.RemoteEndPoint);
                await ProcessRequest(client);
            }
            catch (ObjectDisposedException)
            {
            }
            catch (Exception ex)
            {
                _logger.Info("Error on ProcessRequest " + ex.Message, ex);
            }
            finally
            {
                var copy = CompletedRecievingMessages;
                if (copy != null)
                    copy();
            }
        }

        private async Task ProcessRequest(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            {
                var sender = client.Client.RemoteEndPoint.ToString();
                await new ReceivingProtocolCoordinator(_logger).ReadMessagesAsync(sender, stream, _acceptMessages);
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _listener.Stop();
        }
    }
}