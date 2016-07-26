using System;
using System.Threading.Tasks;
using Microsoft.Reactive.Testing;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests
{
    [Collection("SharedTestDirectory")]
    public class QueueTests : IDisposable
    {
        private readonly SharedTestDirectory _testDirectory;
        private readonly TestScheduler _scheduler;
        private readonly Queue _queue;

        public QueueTests(SharedTestDirectory testDirectory)
        {
            _testDirectory = testDirectory;
            _scheduler = new TestScheduler();
            _queue = ObjectMother.NewQueue(testDirectory.CreateNewDirectoryForTest(), scheduler: _scheduler);
        }

        [Fact]
        public void receive_at_a_later_time()
        {
            var received = false;
            _queue.ReceiveLater(new Message {Queue = "test", Id = MessageId.GenerateRandom()}, TimeSpan.FromSeconds(3));
            using (_queue.Receive("test").Subscribe(x => received = true))
            {
                _scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);
                received.ShouldBeFalse();
                _scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
                received.ShouldBeTrue();
            }
        }

        [Fact]
        public void receive_at_a_specified_time()
        {
            var received = false;
            var time = DateTimeOffset.Now.AddSeconds(5);
            _queue.ReceiveLater(new Message {Queue = "test", Id = MessageId.GenerateRandom()}, time);
            using (_queue.Receive("test").Subscribe(x => received = true))
            {
                _scheduler.AdvanceBy(time.AddSeconds(-3).Ticks);
                received.ShouldBeFalse();
                _scheduler.AdvanceBy(time.AddSeconds(-2).Ticks);
                received.ShouldBeTrue();
            }
        }

        [Fact]
        public void enqueue_a_message()
        {
            Message result = null;
            var expected = ObjectMother.NewMessage<Message>("test");
            using (_queue.Receive("test").Subscribe(x => result = x.Message))
            {
                _queue.Enqueue(expected);
            }
            expected.ShouldBeSameAs(result);
        }

        [Fact]
        public void moving_queues()
        {
            _queue.CreateQueue("another");
            Message first = null;
            Message afterMove = null;
            var expected = ObjectMother.NewMessage<Message>("test");
            using (_queue.Receive("another").Subscribe(x => afterMove = x.Message))
            using (_queue.Receive("test").Subscribe(x => first = x.Message))
            {
                _queue.Enqueue(expected);
                _queue.MoveToQueue("another", first);
            }
            afterMove.Queue.ShouldBe("another");
        }

        [Fact]
        public async Task send_message_to_self()
        {
            using (var queue = ObjectMother.NewQueue(_testDirectory.CreateNewDirectoryForTest()))
            {
                var message = ObjectMother.NewMessage<OutgoingMessage>("test");
                message.Destination = new Uri($"lq.tcp://localhost:{queue.Endpoint.Port}");
                queue.Send(message);
                var received = await queue.Receive("test").FirstAsyncWithTimeout();
                received.ShouldNotBeNull();
                received.Message.Queue.ShouldBe(message.Queue);
                received.Message.Data.ShouldBe(message.Data);
            }
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}