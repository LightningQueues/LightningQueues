using System;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using LightningQueues.Logging;
using LightningQueues.Net.Security;

namespace LightningQueues.Net.Tcp
{
    public class Receiver : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly IReceivingProtocol _protocol;
        private readonly IStreamSecurity _security;
        private readonly ILogger _logger;
        private bool _disposed;
        private readonly Uri _localUri;
        private IObservable<Message> _stream;
        private readonly object _lockObject;
        
        public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol, IStreamSecurity security, ILogger logger)
        {
            Endpoint = endpoint;
            _localUri = new Uri($"lq://localhost:{Endpoint.Port}");
            Timeout = TimeSpan.FromSeconds(5);
            _protocol = protocol;
            _security = security;
            _logger = logger;
            _listener = new TcpListener(Endpoint);
            _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _lockObject = new object();
        }

        public TimeSpan Timeout { get; set; }

        public IPEndPoint Endpoint { get; }

        public IObservable<Message> StartReceiving()
        {
            lock (_lockObject)
            {
                if (_stream != null)
                    return _stream;

                _listener.Start();

                _logger.DebugFormat("TcpListener started listening on port: {0}", Endpoint.Port);
                _stream = Observable.While(IsNotDisposed, ContinueAcceptingNewClients())
                    .Using(x => _protocol.ReceiveStream(
                            _security.Apply(_localUri,  Observable.Return(new NetworkStream(x, false))), 
                            x.RemoteEndPoint.ToString())
                    .Catch((Exception ex) => catchAll(ex)))
                    .Catch((Exception ex) => catchAll(ex))
                    .Publish()
                    .RefCount()
                    .Finally(() => _logger.InfoFormat("TcpListener at {0} has stopped", Endpoint.Port))
                    .Catch((Exception ex) => catchAll(ex));
            }
            return _stream;
        }

        private IObservable<Message> catchAll(Exception ex)
        {
            _logger.Error("Error in message receiving", ex);
            return Observable.Empty<Message>();
        }

        private bool IsNotDisposed()
        {
            return !_disposed;
        }

        private IObservable<Socket> ContinueAcceptingNewClients()
        {
            return Observable.FromAsync(() => _listener.AcceptSocketAsync())
                .Do(x => _logger.DebugFormat("Client at {0} connection established.", x.RemoteEndPoint))
                .Repeat();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _logger.InfoFormat("Disposing TcpListener at {0}", Endpoint.Port);
                _disposed = true;
                _listener.Stop();
            }
        }
    }
}