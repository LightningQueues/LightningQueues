using System;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using LightningQueues.Logging;

namespace LightningQueues.Net.Tcp
{
    public class Receiver : IDisposable
    {
        readonly TcpListener _listener;
        readonly IReceivingProtocol _protocol;
        private readonly ILogger _logger;
        bool _disposed;
        IObservable<Message> _stream;
        private readonly object _lockObject;
        
        public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol, ILogger logger)
        {
            Endpoint = endpoint;
            Timeout = TimeSpan.FromSeconds(5);
            _protocol = protocol;
            _logger = logger;
            _listener = new TcpListener(Endpoint);
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
                    .Using(x => _protocol.ReceiveStream(Observable.Return(new NetworkStream(x, true))))
                    .Publish()
                    .RefCount();
            }
            return _stream;
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
                _disposed = true;
                _listener.Stop();
            }
        }
    }
}