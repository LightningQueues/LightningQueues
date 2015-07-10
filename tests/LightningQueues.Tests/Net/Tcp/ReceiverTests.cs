using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using LightningQueues.Net;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using LightningQueues.Tests.Storage.Lmdb;
using Xunit;

namespace LightningQueues.Tests.Net.Tcp
{
    [Collection("SharedTestDirectory")]
    public class ReceiverTests : IDisposable
    {
        readonly IMessageStore _store;
        readonly SendingProtocol _sender;
        readonly Receiver _receiver;
        readonly IPEndPoint _endpoint;
        readonly RecordingLogger _logger;

        public ReceiverTests(SharedTestDirectory testDirectory)
        {
            var port = PortFinder.FindPort(); //to make it possible to run in parallel
            _endpoint = new IPEndPoint(IPAddress.Loopback, port);
            _logger = new RecordingLogger();
            _sender = new SendingProtocol(_logger);
            _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
            _store.CreateQueue("test");
            var protocol = new ReceivingProtocol(_store, _logger);
            _receiver = new Receiver(_endpoint, protocol);
        }

        [Fact]
        public void stops_listening_on_dispose_of_subscription()
        {
            using(_receiver)
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
                try
                {
                    var listenerShouldThrow = new TcpListener(_endpoint);
                    listenerShouldThrow.Start();
                    true.ShouldBeFalse();
                }
                catch (Exception)
                {
                }
            }
            var listener = new TcpListener(_endpoint);
            listener.Start();
            listener.Stop();
        }

        [Fact]
        public void multiple_subscriptions_are_allowed()
        {
            using (_receiver.StartReceiving().Subscribe(x => { }))
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
        }

        [Fact]
        public void subscribe_unsubscribe_and_subscribe_again()
        {
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
        }

        [Fact]
        public void can_handle_connect_then_disconnect()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using (var client = new TcpClient())
            {
                client.Connect(_endpoint);
            }
        }

        [Fact]
        public void can_handle_sending_three_bytes_then_disconnect()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using (var client = new TcpClient())
            {
                client.Connect(_endpoint);
                client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
            }
        }

        [Fact]
        public void accepts_concurrently_connected_clients()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using(var client1 = new TcpClient())
            using(var client2 = new TcpClient())
            {
                client1.Connect(_endpoint);
                client2.Connect(_endpoint);
                client2.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
                client1.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
            }
        }

        [Fact]
        public async Task receiving_a_valid_message()
        {
            var expected = new OutgoingMessage
            {
                Id = MessageId.GenerateRandom(),
                Queue = "test",
                Data = Encoding.UTF8.GetBytes("hello")
            };
            var messages = new[] {expected};
            var receivingCompletionSource = new TaskCompletionSource<Message>();
            using (_receiver.StartReceiving().Subscribe(x => { receivingCompletionSource.SetResult(x); }))
            using (var client = new TcpClient())
            {
                client.Connect(_endpoint);
                var stream = client.GetStream();
                var outgoing = new OutgoingMessageBatch(new Uri("lq.tcp://localhost"))
                {
                    Messages = messages.ToList(),
                    Stream = stream
                };
                var completionSource = new TaskCompletionSource<bool>();
                using (_sender.SendStream(Observable.Return(outgoing)).Subscribe(x => { completionSource.SetResult(true); }))
                {
                    await Task.WhenAny(completionSource.Task, Task.Delay(100));
                }
                await Task.WhenAny(receivingCompletionSource.Task, Task.Delay(100));
            }
            receivingCompletionSource.Task.IsCompleted.ShouldBeTrue();
            var actual = receivingCompletionSource.Task.Result;
            actual.ShouldNotBeNull();
            actual.Id.ShouldEqual(expected.Id);
            actual.Queue.ShouldEqual(expected.Queue);
            Encoding.UTF8.GetString(actual.Data).ShouldEqual("hello");
        }

        public void Dispose()
        {
            _receiver.Dispose();
            _store.Dispose();
        }
    }
}
