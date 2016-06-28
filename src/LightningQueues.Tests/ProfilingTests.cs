using System;
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
            _sender = ObjectMother.NewLmdbQueue(testDirectory.CreateNewDirectoryForTest(), logger: new NulloLogger());
            _receiver = ObjectMother.NewLmdbQueue(testDirectory.CreateNewDirectoryForTest(), logger: new NulloLogger());
        }

        [Fact, Trait("prof", "explicit")]
        public async Task messages_totaling()
        {
            await messages_totaling_helper(10000);
            //Console.WriteLine("Get new snapshot for comparison and press enter when done.");
            //Console.ReadLine();
        }

        private async Task messages_totaling_helper(int numberOfMessages)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();
            var subscription = _receiver.Receive("test").RunningCount().Subscribe(x =>
            {
                if (x == numberOfMessages)
                    taskCompletionSource.SetResult(true);
            });

            //Console.WriteLine("Get a baseline snapshot for comparison and press enter when done.");
            //Console.ReadLine();
            var destination = new Uri($"lq.tcp://localhost:{_receiver.Endpoint.Port}");
            for (var i = 0; i < numberOfMessages; ++i)
            {
                var message = ObjectMother.NewMessage<OutgoingMessage>("test");
                message.Destination = destination;
                _sender.Send(message);
            }
            await taskCompletionSource.Task;
            subscription.Dispose();
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }
    }
}