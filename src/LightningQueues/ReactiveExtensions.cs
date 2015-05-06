using System;
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
    }
}