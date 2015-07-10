using System;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using LightningQueues.Storage;

namespace LightningQueues.Net.Tcp
{
    public class Sender : IDisposable
    {
        private readonly ISendingProtocol _protocol;
        private readonly IMessageStore _store;
        private readonly ISubject<OutgoingMessage> _outgoing;
        private IDisposable _sendingSubscription;

        public Sender(ISendingProtocol protocol, IMessageStore store)
        {
            _protocol = protocol;
            _store = store;
            _outgoing = new Subject<OutgoingMessage>();
        }

        public IObservable<Message> StartSending()
        {
            var outgoingByEndpoint = _store.PersistedOutgoingMessages()
                .Merge(_outgoing)
                .GroupBy(x => x.Destination)
                .Buffer(TimeSpan.FromMilliseconds(200), TaskPoolScheduler.Default)
                .Select(x => x.Aggregate(new OutgoingMessageBatch(x[0].Key),
                    (batch, item) =>
                    {
                        item.Aggregate(batch, (b, msg) => b.AddMessage(msg));
                        return batch;
                    }));
            var successfullySent = (from outgoing in outgoingByEndpoint
                                   let client = new TcpClient()
                                   from _ in Observable.FromAsync(() => client.ConnectAsync(outgoing.Destination.Host, outgoing.Destination.Port))
                                   select Observable.Using(() => client,
                                   disposable =>
                                   {
                                       outgoing.Stream = client.GetStream();
                                       return _protocol.SendStream(Observable.Return(outgoing));
                                   })).SelectMany(x => x);
            _sendingSubscription = successfullySent.Subscribe(x => { });
            return successfullySent;
        }

        public void Send(OutgoingMessage message)
        {
            _outgoing.OnNext(message);
        }

        public void Dispose()
        {
            if (_sendingSubscription != null)
            {
                _sendingSubscription.Dispose();
                _sendingSubscription = null;
            }
        }
    }
}