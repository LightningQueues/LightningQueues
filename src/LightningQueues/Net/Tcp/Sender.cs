using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace LightningQueues.Net.Tcp
{
    public class Sender : IDisposable
    {
        private readonly ISendingProtocol _protocol;
        private readonly ISubject<OutgoingMessage> _outgoing;
        private readonly ISubject<OutgoingMessageFailure> _failedToSend;
        private readonly IObservable<OutgoingMessage> _initialSeed;
        private IObservable<OutgoingMessage> _successfullySent;
        private IDisposable _sendingSubscription;

        public Sender(ISendingProtocol protocol, IObservable<OutgoingMessage> initialSeed)
        {
            _protocol = protocol;
            _initialSeed = initialSeed;
            _outgoing = new Subject<OutgoingMessage>();
            _failedToSend = new Subject<OutgoingMessageFailure>();
        }

        public IObservable<OutgoingMessage> StartSending()
        {
            _successfullySent = SuccessfullySentMessages().Publish().RefCount();
            _sendingSubscription = _successfullySent.Subscribe(x => { });
            return _successfullySent;
        }

        public IObservable<OutgoingMessage> SuccessfullySentMessages()
        {
            return ConnectedOutgoingMessageBatch()
                .Using(x => _protocol.SendStream(Observable.Return(x))
                .Catch<OutgoingMessage, Exception>(ex => Observable.Empty<OutgoingMessage>()));
        }

        public IObservable<OutgoingMessageBatch> ConnectedOutgoingMessageBatch()
        {
            return AllOutgoingMessagesBatchedByEndpoint()
                .SelectMany(batch =>
                {
                    return Observable.FromAsync(batch.ConnectAsync)
                        .Timeout(TimeSpan.FromSeconds(5))
                        .Catch<Unit, Exception>(ex =>
                        {
                            batch.Dispose();
                            _failedToSend.OnNext(new OutgoingMessageFailure {Batch = batch, Exception = ex});
                            return Observable.Empty<Unit>();
                        })
                        .Select(_ => batch);
                });
        }

        public IObservable<OutgoingMessageBatch> AllOutgoingMessagesBatchedByEndpoint()
        {
            return BufferedAllOutgoingMessages()
                .SelectMany(x =>
                {
                    return x.GroupBy(grouped => grouped.Destination)
                        .Select(grouped => new OutgoingMessageBatch(grouped.Key, grouped, new TcpClient()));
                });
        }

        public IObservable<IList<OutgoingMessage>> BufferedAllOutgoingMessages()
        {
            return AllOutgoingMessages().Buffer(TimeSpan.FromMilliseconds(200))
                .Where(x => x.Count > 0);
        }

        public IObservable<OutgoingMessage> InitialSeed()
        {
            return _initialSeed;
        }

        public IObservable<OutgoingMessage> OnDemandOutgoingMessages()
        {
            return _outgoing;
        }

        public IObservable<OutgoingMessage> AllOutgoingMessages()
        {
            return InitialSeed()
                .Concat(OnDemandOutgoingMessages());
        }


        //private void FailedToSend(Exception ex, OutgoingMessageBatch batch)
        //{
        //    _logger.Info("Failed to connect " + ex);
        //    foreach (var message in batch.Messages)
        //    {
        //        var attempts = _store.FailedToSend(message);
        //        if (ShouldRetry(message, attempts))
        //        {
        //            _scheduler.Schedule(message, TimeSpan.FromSeconds(attempts*attempts),
        //                (sch, state) =>
        //                {
        //                    _outgoing.OnNext(state);
        //                    return Disposable.Empty;
        //                });
        //        }
        //    }
        //}

        public bool ShouldRetry(OutgoingMessage message, int attemptCount)
        {
            return (attemptCount < (message.MaxAttempts ?? 100))
                ||
                DateTime.Now < message.DeliverBy;
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