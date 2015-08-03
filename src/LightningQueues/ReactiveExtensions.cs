using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace LightningQueues
{
    public static class ReactiveExtensions
    {
        public static IObservable<T> Do<T>(this IObservable<T> stream, Func<T, Task> onNext)
        {
            return from s in stream
                   from _ in Observable.FromAsync(() => onNext(s))
                   select s;
        }

        public static IObservable<T> Using<T, TDisposable>(this IObservable<TDisposable> stream, Func<TDisposable, IObservable<T>> action) where TDisposable : IDisposable
        {
            return stream.SelectMany(x =>
            {
                return Observable.Using(() => x, action);
            });
        }

        public static IObservable<T> RetryWithIncreasingDelay<T>(this IObservable<T> stream, int retries, DateTimeOffset? expiration, IScheduler scheduler)
        {
            return RetryWithIncreasingDelay(stream, retries, expiration, 0, scheduler);
        }

        private static IObservable<T> RetryWithIncreasingDelay<T>(this IObservable<T> stream, int retries, DateTimeOffset? expiration, int failedCount, IScheduler scheduler)
        {
            return stream.Catch<T, Exception>(ex =>
            {
                failedCount++;
                if (retries == 0 || (expiration.HasValue && DateTimeOffset.Now > expiration))
                {
                    return Observable.Empty<T>();
                }
                return stream.DelaySubscription(TimeSpan.FromSeconds(failedCount*failedCount), scheduler)
                        .RetryWithIncreasingDelay(--retries, expiration, failedCount, scheduler);
            });
        }
    }
}