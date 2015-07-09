using System;
using System.Net;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage.LMDB;
using LightningQueues.Tests.Storage.Lmdb;
using Xunit;

namespace LightningQueues.Tests
{
    [Collection("SharedTestDirectory")]
    public class QueueTests : IDisposable
    {
        private readonly TestScheduler _scheduler;
        private readonly Queue _queue;

        public QueueTests(SharedTestDirectory testDirectory)
        {
            _scheduler = new TestScheduler();
            var path = testDirectory.CreateNewDirectoryForTest();
            var messageStore = new LmdbMessageStore(path);
            messageStore.CreateQueue("test");
            var port = PortFinder.FindPort();
            var ipEndpoint = new IPEndPoint(IPAddress.Loopback, port);
            var receiver = new Receiver(ipEndpoint, new ReceivingProtocol(messageStore, new RecordingLogger()));
            _queue = new Queue(receiver, messageStore, _scheduler);
        }

        [Fact]
        public void ReceiveAtALaterTime()
        {
            var received = false;
            _queue.ReceiveLater(new IncomingMessage {Queue = "test"}, TimeSpan.FromSeconds(3));
            using (_queue.ReceiveIncoming("test").Subscribe(x => received = true))
            {
                _scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);
                received.ShouldBeFalse();
                _scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
                received.ShouldBeTrue();
            }
        }

        [Fact]
        public void ReceiveAtASpecificTime()
        {
            var received = false;
            var time = DateTimeOffset.Now.AddSeconds(5);
            _queue.ReceiveLater(new IncomingMessage {Queue = "test"}, time);
            using (_queue.ReceiveIncoming("test").Subscribe(x => received = true))
            {
                _scheduler.AdvanceBy(time.AddSeconds(-3).Ticks);
                received.ShouldBeFalse();
                _scheduler.AdvanceBy(time.AddSeconds(-2).Ticks);
                received.ShouldBeTrue();
            }
        }

        [Fact]
        public void EnqueueAMessage()
        {
            IncomingMessage result = null;
            var expected = ObjectMother.NewIncomingMessage("test");
            using (_queue.ReceiveIncoming("test").Subscribe(x => result = x))
            {
                _queue.Enqueue(expected);
            }
            expected.ShouldBeSame(result);
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}