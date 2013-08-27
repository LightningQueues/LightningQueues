using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class OperationsOnUnstartedQueues
    {
        private QueueManager sender, receiver;

        public void SetupReceivedMessages()
        {
            sender = ObjectMother.QueueManager();
            sender.Start();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            receiver = ObjectMother.QueueManager("test2", 23457);
            receiver.CreateQueues("a");

            var wait = new ManualResetEvent(false);
            Action<object, MessageEventArgs> handler = (s,e) => wait.Set();
            receiver.MessageQueuedForReceive += handler;
            
            receiver.Start();

            wait.WaitOne();

            receiver.MessageQueuedForReceive -= handler;
            
            sender.Dispose();
            receiver.Dispose();

            sender = ObjectMother.QueueManager(delete: false);
            receiver = ObjectMother.QueueManager("test2", 23457, delete:false);
            receiver.CreateQueues("a");
        }

        private void SetupUnstarted()
        {
            sender = ObjectMother.QueueManager();
            receiver = ObjectMother.QueueManager("test2", 23457);
            receiver.CreateQueues("a");
        }

        [Test]
        public void Can_send_before_start()
        {
            SetupUnstarted();
            receiver.Start();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }

            sender.Start();

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null, TimeSpan.FromSeconds(5));

                new byte[] { 1, 2, 4, 5 }.ShouldEqual(message.Data);

                tx.Complete();
            }

        }

        [Test]
        public void Can_look_at_sent_messages_without_starting()
        {
            SetupReceivedMessages();

            var messages = sender.GetAllSentMessages();

            1.ShouldEqual(messages.Length);
            new byte[] {6, 7, 8, 9}.ShouldEqual(messages[0].Data);
        }

        [Test]
        public void Can_look_at_messages_queued_for_send_without_starting()
        {
            SetupUnstarted();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }

            var messages = sender.GetMessagesCurrentlySending();

            1.ShouldEqual(messages.Length);
            new byte[] { 1, 2, 4, 5 }.ShouldEqual(messages[0].Data);
        }

        [Test]
        public void Can_receive_queued_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null, TimeSpan.FromSeconds(5));

                new byte[] { 6, 7, 8, 9 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        [Test]
        public void Can_peek_queued_messages_without_starting()
        {
            SetupReceivedMessages();

            var message = receiver.Peek("h", null, TimeSpan.FromSeconds(5));
                
            new byte[] { 6, 7, 8, 9 }.ShouldEqual(message.Data);
        }

        [Test]
        public void Can_move_queued_messages_without_starting()
        {
            SetupReceivedMessages();
            
            using (var tx = new TransactionScope())
            {
                var message = receiver.Peek("h", null, TimeSpan.FromSeconds(5));
                
                receiver.MoveTo("b", message);

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", "b");

                new byte[] { 6, 7, 8, 9 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        [Test]
        public void Can_directly_enqueue_messages_without_starting()
        {
            SetupUnstarted();
            
            using (var tx = new TransactionScope())
            {
                receiver.EnqueueDirectlyTo("h", null, new MessagePayload {Data = new byte[] {8, 6, 4, 2}});

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");

                new byte[] { 8, 6, 4, 2 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        [Test]
        public void Can_get_number_of_messages_without_starting()
        {
            SetupReceivedMessages();

            var numberOfMessagesQueuedForReceive = receiver.GetNumberOfMessages("h");
            1.ShouldEqual(numberOfMessagesQueuedForReceive);
        }

        [Test]
        public void Can_get_messages_without_starting()
        {
            SetupReceivedMessages();

            var messagesQueuedForReceive = receiver.GetAllMessages("h", null);
            1.ShouldEqual(messagesQueuedForReceive.Length);
            new byte[]{ 6, 7, 8 ,9}.ShouldEqual(messagesQueuedForReceive[0].Data);
        }

        [Test]
        public void Can_get_processed_messages_without_starting()
        {
            SetupReceivedMessages();
            using (var tx = new TransactionScope())
            {
                receiver.Receive("h");
                tx.Complete();
            }

            var processedMessages = receiver.GetAllProcessedMessages("h");
            1.ShouldEqual(processedMessages.Length);
            new byte[] { 6, 7, 8, 9 }.ShouldEqual(processedMessages[0].Data);
        }

        [Test]
        public void Can_get_Queues_without_starting()
        {
            SetupUnstarted();
            2.ShouldEqual(receiver.Queues.Length);
        }

        [Test]
        public void Can_get_Subqueues_without_starting()
        {
            SetupUnstarted();

            using (var tx = new TransactionScope())
            {
                receiver.EnqueueDirectlyTo("h", "a", new MessagePayload { Data = new byte[] { 8, 6, 4, 2 } });
                receiver.EnqueueDirectlyTo("h", "b", new MessagePayload { Data = new byte[] { 8, 6, 4, 2 } });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload { Data = new byte[] { 8, 6, 4, 2 } });

                tx.Complete();
            }

            3.ShouldEqual(receiver.GetSubqueues("h").Length);
        }

        [Test]
        public void Can_get_Queue_without_starting()
        {
            SetupUnstarted();
            Assert.NotNull(receiver.GetQueue("h"));
        }

        [TearDown]
        public void Teardown()
        {
            sender.Dispose();
            receiver.Dispose();
        }
    }
}
