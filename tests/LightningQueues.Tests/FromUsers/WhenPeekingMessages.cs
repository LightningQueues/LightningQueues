using System;
using System.IO;
using System.Net;
using System.Transactions;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests.FromUsers
{
    [TestFixture]
    public class WhenPeekingMessages 
    {
        private QueueManager queueManager;

        [SetUp]
        public void Setup()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
        }

        [Test]
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

        [TearDown]
        public void Teardown()
        {
            queueManager.Dispose();
        }
    }
}