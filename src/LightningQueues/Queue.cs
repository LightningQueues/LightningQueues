using System;
using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;

namespace LightningQueues
{
    public class Queue : IDisposable
    {
        private readonly Sender _sender;
        private readonly Receiver _receiver;
        private readonly IMessageStore _messageStore;
        private readonly Subject<Message> _receiveSubject;
        private readonly IScheduler _scheduler;

        public Queue(Receiver receiver, Sender sender, IMessageStore messageStore) : this(receiver, sender, messageStore, TaskPoolScheduler.Default)
        {
        }

        public Queue(Receiver receiver, Sender sender, IMessageStore messageStore, IScheduler scheduler)
        {
            _receiver = receiver;
            _sender = sender;
            _messageStore = messageStore;
            _receiveSubject = new Subject<Message>();
            _scheduler = scheduler;
            _sender.StartSending();
        }

        public IPEndPoint Endpoint => _receiver.Endpoint;

        internal IMessageStore Store => _messageStore;

        internal ISubject<Message> ReceiveLoop => _receiveSubject;

        public IObservable<MessageContext> ReceiveIncoming(string queueName)
        {
            return _messageStore.PersistedMessages(queueName)
                .Concat(_receiver.StartReceiving())
                .Merge(_receiveSubject)
                .Where(x => x.Queue == queueName)
                .Select(x => new MessageContext(x, this));
        }

        public void MoveToQueue(string queueName, Message message)
        {
            var tx = _messageStore.BeginTransaction();
            _messageStore.MoveToQueue(tx, queueName, message);
            tx.Commit();
            message.Queue = queueName;
            _receiveSubject.OnNext(message);
        }

        public void Enqueue(Message message)
        {
            var tx = _messageStore.BeginTransaction();
            _messageStore.StoreMessages(tx, message);
            tx.Commit();
            _receiveSubject.OnNext(message);
        }

        public void ReceiveLater(Message message, TimeSpan timeSpan)
        {
            _scheduler.Schedule(message, timeSpan, (sch, msg) =>
            {
                _receiveSubject.OnNext(msg);
                return Disposable.Empty;
            });
        }

        public void Send(OutgoingMessage message)
        {
            var tx = _messageStore.BeginTransaction();
            _messageStore.StoreOutgoing(tx, message);
            tx.Commit();
            _sender.Send(message);
        }

        public void ReceiveLater(Message message, DateTimeOffset time)
        {
            _scheduler.Schedule(message, time, (sch, msg) =>
            {
                _receiveSubject.OnNext(msg);
                return Disposable.Empty;
            });
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
            _receiveSubject.Dispose();
            _messageStore.Dispose();
        }
    }
}