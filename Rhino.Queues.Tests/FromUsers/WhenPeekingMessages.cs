using System.IO;
using System.Net;
using System.Transactions;
using Xunit;

namespace Rhino.Queues.Tests.FromUsers
{
    public class WhenPeekingMessages
    {
        private readonly QueueManager queueManager;

        public WhenPeekingMessages()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            queueManager.CreateQueues("h");
            queueManager.Start();
        }

        [Fact]
        public void ItShouldNotDecrementMessageCount()
        {
            using (var tx = new TransactionScope())
            {
                queueManager.EnqueueDirectlyTo("h", null, new MessagePayload
                {
                    Data = new byte[] { 1, 2, 4, 5 }
                });
                tx.Complete();
            }
            var count = queueManager.GetNumberOfMessages("h");
            Assert.Equal(1, count);
            var msg = queueManager.Peek("h");
            Assert.Equal(new byte[] { 1, 2, 4, 5 }, msg.Data);
            count = queueManager.GetNumberOfMessages("h");
            Assert.Equal(1, count); 
        }
    }
}