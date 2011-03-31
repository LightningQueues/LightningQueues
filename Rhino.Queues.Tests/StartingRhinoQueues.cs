using System;
using System.IO;
using System.Net;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    class StartingRhinoQueues : WithDebugging, IDisposable
    {
        private readonly QueueManager queueManager;

        public StartingRhinoQueues()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
        }

        [Fact]
        public void Starting_twice_should_throw()
        {
            queueManager.Start();

            Assert.Throws<InvalidOperationException>(() => queueManager.Start());
        }

        [Fact]
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
