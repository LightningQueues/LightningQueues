using System;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Disposables;

namespace LightningQueues.Net.Tcp
{
    public class Receiver : IDisposable
    {
        readonly TcpListener _listener;
        readonly IReceivingProtocol _protocol;
        bool _disposed;
        IObservable<Message> _stream;
        private readonly object _lockObject;
        
        public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol)
        {
            Endpoint = endpoint;
            Timeout = TimeSpan.FromSeconds(5);
            _protocol = protocol;
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

                _stream = Observable.While(() => !_disposed, Observable.FromAsync(_listener.AcceptTcpClientAsync)
                    .Repeat()
                    .Select(x => new {Stream = x.GetStream(), Client = x})
                    .SelectMany(x =>
                    {
                        return Observable.Using(() => new CompositeDisposable(x.Client, x.Stream),
                            disposable => _protocol.ReceiveStream(Observable.Return(x.Stream)));
                    })
                    .Publish()
                    .RefCount());
            }
            return _stream;
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