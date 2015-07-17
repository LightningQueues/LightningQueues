using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace LightningQueues.Tests
{
    public static class ReactiveExtensions
    {
        public static async Task<T> FirstAsyncWithTimeout<T>(this IObservable<T> stream, TimeSpan timeSpan)
        {
            var completionSource = new TaskCompletionSource<T>();
            using (stream.Subscribe(x => completionSource.SetResult(x)))
            using (Observable.Interval(timeSpan).Subscribe(x => completionSource.SetException(new TimeoutException())))
            {
                return await completionSource.Task;
            }
        }

        public static Task<T> FirstAsyncWithTimeout<T>(this IObservable<T> stream)
        {
            return stream.FirstAsyncWithTimeout(TimeSpan.FromSeconds(1));
        }

        public static IObservable<int> RunningCount<T>(this IObservable<T> stream)
        {
            return stream.Scan(0, (acc, current) => acc + 1);
        }
    }
}