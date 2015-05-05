using System;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Disposables;

namespace LightningQueues.Net.Tcp
{
    public class Receiver
    {
        readonly IPEndPoint _endpoint;
        readonly TcpListener _listener;
        readonly IReceivingProtocol _protocol;
        bool _started;
        IObservable<IncomingMessage> _stream;
        
        public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol)
        {
            _endpoint = endpoint;
            Timeout = TimeSpan.FromSeconds(5);
            _protocol = protocol;
            _listener = new TcpListener(_endpoint);
        }

        public TimeSpan Timeout { get; set; }

        public IObservable<IncomingMessage> StartReceiving()
        {
            if(_started)
                return _stream;

            _listener.Start();
                             
            _stream = Observable.FromAsync(_listener.AcceptTcpClientAsync)
                .Repeat()
                .Select(x => new { Stream = x.GetStream(), Client = x })
                .SelectMany(x =>
                {
                    return Observable.Using(() => new CompositeDisposable(x.Client, x.Stream),
                                            disposable => _protocol.ReceiveStream(Observable.Return(x.Stream)));
                })
                .Finally(() => _listener.Stop()).Finally(() => _started = false)
                .Publish()
                .RefCount();
            return _stream;
        }
     }
}