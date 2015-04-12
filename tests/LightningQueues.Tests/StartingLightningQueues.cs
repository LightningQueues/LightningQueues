using System;
using Xunit;

namespace LightningQueues.Tests
{
    public class StartingLightningQueues : IDisposable
    {
        private QueueManager queueManager;

        public StartingLightningQueues()
        {
            queueManager = ObjectMother.QueueManager();
        }

        [Fact(Skip="Not on mono")]
        public void Starting_twice_should_throw()
        {
            queueManager.Start();

            Assert.Throws<InvalidOperationException>(() => queueManager.Start());
        }

        [Fact(Skip="Not on mono")]
        public void Starting_after_dispose_should_throw()
        {
            queueManager.Dispose();

            Assert.Throws<ObjectDisposedException>(() => queueManager.Start());
        }

        public void Dispose()
        {
            queueManager.Dispose();
        }
    }
}
