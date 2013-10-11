using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Model;

namespace LightningQueues.Protocol
{
    public class Receiver : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IPEndPoint _endpointToListenTo;
        private readonly Func<Message[], IMessageAcceptance> _acceptMessages;
        private TcpListener _listener;
        private bool _disposed;

        public Receiver(IPEndPoint endpointToListenTo, Func<Message[], IMessageAcceptance> acceptMessages, ILogger logger)
        {
            _endpointToListenTo = endpointToListenTo;
            _acceptMessages = acceptMessages;
            _logger = logger;
        }

        public void Start()
        {
            _logger.Debug("Starting to listen on {0}", _endpointToListenTo);
            TryStart(_endpointToListenTo);
            StartAccepting();
            _logger.Debug("Now listen on {0}", _endpointToListenTo);
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
            try
            {
                _listener = new TcpListener(endpointToListenTo);
                _listener.Start();
            }
            catch (SocketException ex)
            {
                if (ex.Message == "Only one usage of each socket address (protocol/network address/port) is normally permitted")
                {
                    throw new EndpointInUseException(endpointToListenTo, ex);
                }
            }
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
        }

        private async Task ProcessRequest(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            {
                var sender = client.Client.RemoteEndPoint.ToString();
                await new ReceivingProtocol(_logger).ReadMessagesAsync(sender, stream, _acceptMessages);
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _listener.Stop();
        }
    }
}