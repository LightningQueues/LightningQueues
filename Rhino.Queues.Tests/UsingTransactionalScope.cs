using System;
using System.Text;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class UsingTransactionalScope : IDisposable
    {
         private readonly QueueManager queueManager;

        public UsingTransactionalScope()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
        }

        [Fact]
        public void can_receive_message()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope =  queueManager.BeginTransactionalScope();
            var message = transactionalScope.Receive("h");
            Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            transactionalScope.Commit();

            var transactionalScope2 =  queueManager.BeginTransactionalScope();
            Assert.Throws<TimeoutException>(() => transactionalScope2.Receive("h", TimeSpan.Zero));
        }

        [Fact]
        public void previous_dequeue_with_rollback_can_be_dequeued_again()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope = queueManager.BeginTransactionalScope();
            var message = transactionalScope.Receive("h");
            Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            transactionalScope.Rollback();

            var transactionalScope2 = queueManager.BeginTransactionalScope();
            message = transactionalScope2.Receive("h");
            Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
        }

        [Fact]
        public void dequeue_success_moves_to_history()
        {
            var sender = ObjectMother.Sender();
            sender.Send();

            var transactionalScope = queueManager.BeginTransactionalScope();
            transactionalScope.Receive("h");
            transactionalScope.Commit();

            
            var messages = queueManager.GetAllProcessedMessages("h");
            Assert.Equal(1, messages.Length);
            Assert.Equal("hello", Encoding.Unicode.GetString(messages[0].Data));
        }

        [Fact]
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

        public void Dispose()
        {
            queueManager.Dispose();
        }
    }
}