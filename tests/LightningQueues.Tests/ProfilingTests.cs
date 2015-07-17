using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using Xunit;

namespace LightningQueues.Tests
{
    [Collection("SharedTestDirectory")]
    public class ProfilingTests : IDisposable
    {
        private readonly Queue _sender;
        private readonly Queue _receiver;

        public ProfilingTests(SharedTestDirectory testDirectory)
        {
            _sender = ObjectMother.NewLmdbQueue(testDirectory.CreateNewDirectoryForTest(), logger: new NLogLogger());
            _receiver = ObjectMother.NewLmdbQueue(testDirectory.CreateNewDirectoryForTest(), logger: new NLogLogger());
        }

        [InlineData(200)] //turn this up after retry logic and sending choke is complete
        [Theory, Trait("prof", "explicit")]
        public async Task messages_totaling(int numberOfMessages)
        {
            await messages_totaling_helper(numberOfMessages);
            //Console.WriteLine("Get new snapshot for comparison and press enter when done.");
            //Console.ReadLine();
        }

        private async Task messages_totaling_helper(int numberOfMessages)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();
            var subscription = _receiver.ReceiveIncoming("test").RunningCount().Subscribe(x =>
            {
                if (x == numberOfMessages)
                    taskCompletionSource.SetResult(true);
            });
            //Console.WriteLine("Get a baseline snapshot for comparison and press enter when done.");
            //Console.ReadLine();
            var destination = new Uri($"lq.tcp://localhost:{_receiver.Endpoint.Port}");
            var generator = Observable.Range(0, numberOfMessages).Buffer(TimeSpan.FromMilliseconds(100)).Subscribe(x =>
            {
                foreach (var item in x)
                {
                    var message = ObjectMother.NewMessage<OutgoingMessage>("test");
                    message.Id = MessageId.GenerateRandom();
                    message.Destination = destination;
                    _sender.Send(message);
                }
            });
            await taskCompletionSource.Task;
            generator.Dispose();
            subscription.Dispose();
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }
    }
}