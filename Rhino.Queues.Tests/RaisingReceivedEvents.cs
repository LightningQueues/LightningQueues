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
        private MessageEventArgs messageEventArgs2;
        private int messageEventCount;
        private int messageEventCount2;

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
            receiver.CreateQueues("h", "b");
            ResetEventRecorder();
            return receiver;
        }

        private void ResetEventRecorder()
        {
            messageEventArgs = null;
            messageEventArgs2 = null;
            messageEventCount = 0;
            messageEventCount2 = 0;
        }

        void RecordMessageEvent(object s, MessageEventArgs e)
        {
            messageEventArgs = e;
            
            messageEventCount++;
        }

        void RecordMessageEvent2(object s, MessageEventArgs e)
        {
            messageEventArgs2 = e;
            messageEventCount2++;
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

                    while (messageEventCount == 0)
                        Thread.Sleep(100);
                }
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageQueuedForReceive_EventIsRaised_DirectEnqueuing()
        {
            using (var receiver = SetupReciever())
            {
                receiver.MessageQueuedForReceive += RecordMessageEvent;

                using (var tx = new TransactionScope())
                {
                    receiver.EnqueueDirectlyTo("h",null, new MessagePayload{Data = new byte[]{1,2,3}});

                    tx.Complete();
                }
                Thread.Sleep(1000);
            }

            Assert.NotNull(messageEventArgs);
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

        [Fact]
        public void MessageReceived_and_MessageQueuedForReceive_events_raised_when_message_removed_and_moved()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageReceived += RecordMessageEvent;
                    receiver.MessageQueuedForReceive += RecordMessageEvent2;

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

                    while (messageEventCount2 == 0)
                        Thread.Sleep(100);

                    ResetEventRecorder();

                    using (var tx = new TransactionScope())
                    {
                        var message = receiver.Receive("h");
                        receiver.MoveTo("b", message);
                        tx.Complete();
                    }

                    Assert.Equal(1, messageEventCount);
                    Assert.NotNull(messageEventArgs);
                    Assert.Equal("h", messageEventArgs.Message.Queue);
                    Assert.Null(messageEventArgs.Message.SubQueue);

                    Assert.Equal(1, messageEventCount2);
                    Assert.NotNull(messageEventArgs2);
                    Assert.Equal("h", messageEventArgs2.Message.Queue);
                    Assert.Equal("b", messageEventArgs2.Message.SubQueue);
                }
            }
        }

        [Fact]
        public void MessageReceived_and_MessageQueuedForReceive_events_raised_when_message_peeked_and_moved()
        {
            using (var sender = SetupSender())
            {
                using (var receiver = SetupReciever())
                {
                    receiver.MessageReceived += RecordMessageEvent;
                    receiver.MessageQueuedForReceive += RecordMessageEvent2;

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

                    while (messageEventCount2 == 0)
                        Thread.Sleep(100);

                    ResetEventRecorder();

                    using (var tx = new TransactionScope())
                    {
                        var message = receiver.Peek("h");
                        receiver.MoveTo("b", message);
                        tx.Complete();
                    }

                    Assert.Equal(1, messageEventCount);
                    Assert.NotNull(messageEventArgs);
                    Assert.Equal("h", messageEventArgs.Message.Queue);
                    Assert.Null(messageEventArgs.Message.SubQueue);

                    Assert.Equal(1, messageEventCount2);
                    Assert.NotNull(messageEventArgs2);
                    Assert.Equal("h", messageEventArgs2.Message.Queue);
                    Assert.Equal("b", messageEventArgs2.Message.SubQueue);
                }
            }
        }
    }
}