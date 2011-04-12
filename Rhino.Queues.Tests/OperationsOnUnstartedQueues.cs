using System;
using System.IO;
using System.Net;
using System.Transactions;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class OperationsOnUnstartedQueues : WithDebugging, IDisposable
    {
        private QueueManager sender, receiver;

        public void SetupReceivedMessages()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            if (Directory.Exists("test2.esent"))
                Directory.Delete("test2.esent", true);

            sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            sender.Start();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), "test2.esent");
            receiver.CreateQueues("h", "a");
            receiver.Start();

            using (var tx = new TransactionScope())
            {
                receiver.Receive("h", null, TimeSpan.FromSeconds(5));

                tx.Complete();
            }

            sender.Dispose();
            receiver.Dispose();

            sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), "test2.esent");
            receiver.CreateQueues("h", "a");
        }

        private void SetupUnstarted()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            if (Directory.Exists("test2.esent"))
                Directory.Delete("test2.esent", true);

            sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), "test2.esent");
            receiver.CreateQueues("h", "a");
        }

        [Fact]
        public void Can_send_before_start()
        {
            SetupUnstarted();
            receiver.Start();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
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

                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                tx.Complete();
            }

        }

        [Fact]
        public void Can_look_at_sent_messages_without_starting()
        {
            SetupReceivedMessages();

            var messages = sender.GetAllSentMessages();
            Assert.Equal(2, messages.Length);
        }

        [Fact]
        public void Can_look_at_messages_queued_for_send_without_starting()
        {
            SetupUnstarted();

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }

            var messages = sender.GetMessagesCurrentlySending();
            Assert.Equal(1, messages.Length);
            Assert.Equal(new byte[] { 1, 2, 4, 5 }, messages[0].Data);
        }

        [Fact]
        public void Can_receive_queued_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null, TimeSpan.FromSeconds(5));

                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
        public void Can_peek_queued_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var message = receiver.Peek("h", null, TimeSpan.FromSeconds(5));
                
                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
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

                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
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

                Assert.Equal(new byte[] { 8, 6, 4, 2 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
        public void Can_get_number_of_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var numberOfMessagesQueuedForReceive = receiver.GetNumberOfMessages("h");
                Assert.Equal(1, numberOfMessagesQueuedForReceive);

                tx.Complete();
            }
        }

        [Fact]
        public void Can_get_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var messagesQueuedForReceive = receiver.GetAllMessages("h", null);
                Assert.Equal(1, messagesQueuedForReceive.Length);
                Assert.Equal(new byte[]{ 6, 7, 8 ,9}, messagesQueuedForReceive[0].Data);

                tx.Complete();
            }
        }

        [Fact]
        public void Can_get_processed_messages_without_starting()
        {
            SetupReceivedMessages();

            using (var tx = new TransactionScope())
            {
                var processedMessages = receiver.GetAllProcessedMessages("h");
                Assert.Equal(1, processedMessages.Length);
                Assert.Equal(new byte[] { 1, 2, 4, 5 }, processedMessages[0].Data);
                tx.Complete();
            }
        }

        [Fact]
        public void Can_get_Queues_without_starting()
        {
            SetupUnstarted();
            Assert.Equal(2, receiver.Queues.Length);
        }

        [Fact]
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

            Assert.Equal(3, receiver.GetSubqueues("h").Length);
        }

        [Fact]
        public void Can_get_Queue_without_starting()
        {
            SetupUnstarted();
            Assert.NotNull(receiver.GetQueue("h"));
        }

        public void Dispose()
        {
            sender.Dispose();
            receiver.Dispose();
        }
    }
}
