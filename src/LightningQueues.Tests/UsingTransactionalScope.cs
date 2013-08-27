using System;
using System.Text;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class UsingTransactionalScope
    {
        private QueueManager queueManager;

        [SetUp]
        public void Setup()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
        }

        [Test]
        public void can_receive_message()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope = queueManager.BeginTransactionalScope();
            var message = transactionalScope.Receive("h");
            "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));
            transactionalScope.Commit();

            var transactionalScope2 = queueManager.BeginTransactionalScope();
            Assert.Throws<TimeoutException>(() => transactionalScope2.Receive("h", TimeSpan.Zero));
        }

        [Test]
        public void previous_dequeue_with_rollback_can_be_dequeued_again()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope = queueManager.BeginTransactionalScope();
            var message = transactionalScope.Receive("h");
            "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));
            transactionalScope.Rollback();

            var transactionalScope2 = queueManager.BeginTransactionalScope();
            message = transactionalScope2.Receive("h");
            "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));
        }

        [Test]
        public void dequeue_success_moves_to_history()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope = queueManager.BeginTransactionalScope();
            transactionalScope.Receive("h");
            transactionalScope.Commit();


            var messages = queueManager.GetAllProcessedMessages("h");
            1.ShouldEqual(messages.Length);
            "hello".ShouldEqual(Encoding.Unicode.GetString(messages[0].Data));
        }

        [Test]
        public void rollback_on_send_does_not_send_message()
        {
            var sender = ObjectMother.QueueManager("test2", 23457);
            var sendingScope = sender.BeginTransactionalScope();
            sendingScope.Send(ObjectMother.UriFor(23456), ObjectMother.MessagePayload());
            sendingScope.Rollback();

            var receivingScope = queueManager.BeginTransactionalScope();
            Assert.Throws<TimeoutException>(() => receivingScope.Receive("h", TimeSpan.FromSeconds(1)));
            sender.Dispose();
        }

        [TearDown]
        public void Teardown()
        {
            queueManager.Dispose();
        }
    }
}