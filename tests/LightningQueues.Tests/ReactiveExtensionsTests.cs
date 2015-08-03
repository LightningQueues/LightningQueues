using System;
using System.Reactive.Linq;
using Xunit;

namespace LightningQueues.Tests
{
    public class ReactiveExtensionsTests
    {
        [Fact]
        public void retrying_on_error_no_expiration()
        {
            var testScheduler = new TestScheduler();
            var stream = Observable.Range(1, 3)
                .ThrowTimes(1)
                .RetryWithIncreasingDelay(1, null, testScheduler);
            var initialTime = testScheduler.Now;
            var results = testScheduler.Start(() => stream, 
                TimeSpan.FromSeconds(1).Ticks, 
                TimeSpan.FromSeconds(2).Ticks,
                TimeSpan.FromSeconds(30).Ticks);
            //difference.ShouldEqual(TimeSpan.FromSeconds(1).Ticks);
            //foreach (var result in results.Messages)
            //{
            //    Console.WriteLine(result);
            //}
        }
    }
}