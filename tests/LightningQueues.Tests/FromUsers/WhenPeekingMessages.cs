using System;
using System.Transactions;
using FubuTestingSupport;
using Xunit;

namespace LightningQueues.Tests.FromUsers
{
    public class WhenPeekingMessages : IDisposable
    {
        private QueueManager queueManager;

        public WhenPeekingMessages()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
        }

        [Fact(Skip="Not on mono")]
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
            1.ShouldEqual(count);
            var msg = queueManager.Peek("h");
            new byte[] { 1, 2, 4, 5 }.ShouldEqual(msg.Data);
            count = queueManager.GetNumberOfMessages("h");
            1.ShouldEqual(count); 
        }

        public void Dispose()
        {
            queueManager.Dispose();
        }
    }
}