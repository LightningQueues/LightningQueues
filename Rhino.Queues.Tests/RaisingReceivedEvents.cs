using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    class RaisingReceivedEvents : WithDebugging
    {
        private const string TEST_QUEUE_1 = "testA.esent";
        private const string TEST_QUEUE_2 = "testB.esent";

        private MessageEventArgs messageEventArgs;

        public QueueManager SetupSender()
        {
            if (Directory.Exists(TEST_QUEUE_1))
                Directory.Delete(TEST_QUEUE_1, true);

            var sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1);
            return sender;
        }

        public QueueManager SetupReciever()
        {
            if (Directory.Exists(TEST_QUEUE_2))
                Directory.Delete(TEST_QUEUE_2, true);

            var receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2);
            receiver.CreateQueues("h");
            messageEventArgs = null;
            return receiver;
        }

        void RecordMessageEvent(object s, MessageEventArgs e)
        {
            messageEventArgs = e;
        }

        [Fact]
        public void MessageQueuedForReceive_EventIsRaised()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageQueuedForReceive += RecordMessageEvent;

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
                    Thread.Sleep(1000);
                }
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("127.0.0.1", messageEventArgs.Endpoint.Host);
            Assert.Equal(23457, messageEventArgs.Endpoint.Port);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        //Unable to find a clean way of causing receive to abort;
        //This test only passes if I deliberately break the acknowledgment sending code in Sender.
        //[Fact]
        public void MessageQueuedForReceive_EventNotRaised_IfReceiveAborts()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageQueuedForReceive += RecordMessageEvent;

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
                    Thread.Sleep(1000);
                }
            }

            Assert.Null(messageEventArgs);
        }

        [Fact]
        public void MessageReceived_EventIsRaised()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageReceived += RecordMessageEvent;

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
                    Thread.Sleep(1000);

                    using (var tx = new TransactionScope())
                    {
                        receiver.Receive("h");
                        tx.Complete();
                    }
                }
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("127.0.0.1", messageEventArgs.Endpoint.Host);
            Assert.Equal(23457, messageEventArgs.Endpoint.Port);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageReceived_EventNotRaised_IfMessageNotReceived()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageReceived += RecordMessageEvent;

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
                    Thread.Sleep(1000);
                }
            }

            Assert.Null(messageEventArgs);
        }
    }
}