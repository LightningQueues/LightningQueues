using System;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using LightningQueues.Storage;

namespace LightningQueues.Net
{
    public class SendingErrorPolicy : IDisposable
    {
        private readonly ISubject<OutgoingMessage> _retrySubject;
        private readonly IDisposable _errorSubscription;

        public SendingErrorPolicy(IMessageStore store, IScheduler scheduler, IObservable<OutgoingMessageFailure> failedToConnect)
        {
            _retrySubject = new Subject<OutgoingMessage>();
            _errorSubscription = failedToConnect.Subscribe(x =>
            {
                foreach (var message in x.Batch.Messages)
                {
                    var count = store.FailedToSend(message);
                    if (ShouldRetry(message, count))
                    {
                        scheduler.Schedule(x, TimeSpan.FromSeconds(count*count), (sch, state) =>
                        {
                            _retrySubject.OnNext(message);
                            return Disposable.Empty;
                        });
                    }
                }
            });
        }

        public IObservable<OutgoingMessage> RetryStream => _retrySubject;

        public bool ShouldRetry(OutgoingMessage message, int attemptCount)
        {
            return (attemptCount < (message.MaxAttempts ?? 100))
                &&
                (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
        }

        public void Dispose()
        {
            _errorSubscription.Dispose();
        }
    }
}