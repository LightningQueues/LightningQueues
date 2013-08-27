using System;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class StartingLightningQueues
    {
        private QueueManager queueManager;

        [SetUp]
        public void Setup()
        {
            queueManager = ObjectMother.QueueManager();
        }

        [Test]
        public void Starting_twice_should_throw()
        {
            queueManager.Start();

            Assert.Throws<InvalidOperationException>(() => queueManager.Start());
        }

        [Test]
        public void Starting_after_dispose_should_throw()
        {
            queueManager.Dispose();

            Assert.Throws<ObjectDisposedException>(() => queueManager.Start());
        }

        [TearDown]
        public void Teardown()
        {
            queueManager.Dispose();
        }
    }
}
