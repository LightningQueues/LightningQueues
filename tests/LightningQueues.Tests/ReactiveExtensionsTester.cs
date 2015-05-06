using System;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Xunit;

namespace LightningQueues.Tests
{
    public class ReactiveExtensionsTester
    {
        [Fact]
        public void doing_async()
        {
            Observable.Return(1).Do(DoSomethingAsync)
                .SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(x => Assert.Equal(1, x))
                .Dispose();
        }

        private async Task DoSomethingAsync(int number)
        {
            await Task.Delay(1);
        }

        [Fact]
        public void doing_async_with_exceptions()
        {
            Observable.Return(1).Do(DoSomethingAsyncThrowing)
                .Materialize()
                .SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(x => Assert.Equal(NotificationKind.OnError, x.Kind))
                .Dispose();
        }

        private async Task DoSomethingAsyncThrowing(int number)
        {
            await Task.Delay(1);
            throw new Exception();
        }
    }
}