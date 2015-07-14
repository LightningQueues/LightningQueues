using System;
using System.Net;
using System.Threading.Tasks;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;
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
        private readonly IMessageStore _store;
        private readonly int _port;

        public QueueTests(SharedTestDirectory testDirectory)
        {
            _scheduler = new TestScheduler();
            var path = testDirectory.CreateNewDirectoryForTest();
            _store = new LmdbMessageStore(path);
            _store.CreateQueue("test");
            var logger = new RecordingLogger();
            _port = PortFinder.FindPort();
            var ipEndpoint = new IPEndPoint(IPAddress.Loopback, _port);
            var receiver = new Receiver(ipEndpoint, new ReceivingProtocol(_store, logger), logger);
            var sender = new Sender(new SendingProtocol(logger), _store, logger);
            _queue = new Queue(receiver, sender, _store, _scheduler);
        }

        [Fact]
        public void receive_at_a_later_time()
        {
            var received = false;
            _queue.ReceiveLater(new Message {Queue = "test"}, TimeSpan.FromSeconds(3));
            using (_queue.ReceiveIncoming("test").Subscribe(x => received = true))
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
            _queue.ReceiveLater(new Message {Queue = "test"}, time);
            using (_queue.ReceiveIncoming("test").Subscribe(x => received = true))
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
            using (_queue.ReceiveIncoming("test").Subscribe(x => result = x.Message))
            {
                _queue.Enqueue(expected);
            }
            expected.ShouldBeSame(result);
        }

        [Fact]
        public void moving_queues()
        {
            _store.CreateQueue("another");
            Message first = null;
            Message afterMove = null;
            var expected = ObjectMother.NewMessage<Message>("test");
            using (_queue.ReceiveIncoming("another").Subscribe(x => afterMove = x.Message))
            using (_queue.ReceiveIncoming("test").Subscribe(x => first = x.Message))
            {
                _queue.Enqueue(expected);
                _queue.MoveToQueue("another", first);
            }
            afterMove.Queue.ShouldEqual("another");
        }

        [Fact]
        public async Task send_message_to_self()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>("test");
            message.Destination = new Uri($"lq.tcp://localhost:{_port}");
            _queue.Send(message);
            var received = await _queue.ReceiveIncoming("test").FirstAsyncWithTimeout();
            received.ShouldNotBeNull();
            received.Message.Queue.ShouldEqual(message.Queue);
            received.Message.Data.ShouldEqual(message.Data);
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}