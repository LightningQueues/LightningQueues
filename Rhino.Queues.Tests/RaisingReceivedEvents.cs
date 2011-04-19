using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class RaisingReceivedEvents : WithDebugging, IDisposable
    {
        private const string TEST_QUEUE_1 = "testA.esent";
        private const string TEST_QUEUE_2 = "testB.esent";

        private MessageEventArgs messageEventArgs;
        private MessageEventArgs messageEventArgs2;
        private int messageEventCount;
        private int messageEventCount2;
        private QueueManager lastCreatedSender;
        private QueueManager lastCreatedReceiver;

        public QueueManager SetupSender()
        {
            //Needed because tests that are terminated by XUnit due to a timeout
            //are terminated rudely such that using statements do not dispose of their objects.
            if (lastCreatedSender != null)
                lastCreatedSender.Dispose();

            if (Directory.Exists(TEST_QUEUE_1))
                Directory.Delete(TEST_QUEUE_1, true);

            lastCreatedSender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1);
            lastCreatedSender.Start();
            return lastCreatedSender;
        }

        public QueueManager SetupReciever()
        {
            //Needed because tests that are terminated by XUnit due to a timeout
            //are terminated rudely such that using statements do not dispose of their objects.
            if (lastCreatedReceiver != null)
                lastCreatedReceiver.Dispose();

            if (Directory.Exists(TEST_QUEUE_2))
                Directory.Delete(TEST_QUEUE_2, true);

            lastCreatedReceiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2);
            lastCreatedReceiver.CreateQueues("h", "b");
            lastCreatedReceiver.Start();
            ResetEventRecorder();
            return lastCreatedReceiver;
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

        [Fact(Timeout = 5000)]
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

                    receiver.MessageQueuedForReceive -= RecordMessageEvent;
                }
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact(Timeout = 5000)]
        public void MessageQueuedForReceive_EventIsRaised_DirectEnqueuing()
        {
            using (var receiver = SetupReciever())
            {
                receiver.MessageQueuedForReceive += RecordMessageEvent;

                using (var tx = new TransactionScope())
                {
                    receiver.EnqueueDirectlyTo("h", null, new MessagePayload { Data = new byte[] { 1, 2, 3 } });

                    tx.Complete();
                }
                while (messageEventCount == 0)
                    Thread.Sleep(100);

                receiver.MessageQueuedForReceive -= RecordMessageEvent;
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageQueuedForReceive_EventNotRaised_IfReceiveAborts()
        {
            ManualResetEvent wait = new ManualResetEvent(false);

            using (var sender = new FakeSender
            {
                Destination = new Endpoint("localhost", 23457),
                FailToAcknowledgeReceipt = true,
                Messages = new[] { new Message
                                        {
                                            Id = new MessageId{ MessageIdentifier = Guid.NewGuid(), SourceInstanceId = Guid.NewGuid()},
                                            SentAt = DateTime.Now,
                                            Queue = "h", 
                                            Data = new byte[] { 1, 2, 4, 5 }
                                        } }
            })
            {

                sender.SendCompleted += () => wait.Set();
                using (var receiver = SetupReciever())
                {
                    receiver.MessageQueuedForReceive += RecordMessageEvent;

                    sender.Send();
                    wait.WaitOne();

                    Thread.Sleep(1000);

                    receiver.MessageQueuedForReceive -= RecordMessageEvent;
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
                    sender.WaitForAllMessagesToBeSent();

                    using (var tx = new TransactionScope())
                    {
                        receiver.Receive("h");
                        tx.Complete();
                    }

                    receiver.MessageReceived -= RecordMessageEvent;
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

                    receiver.MessageReceived -= RecordMessageEvent;
                }
            }

            Assert.Null(messageEventArgs);
        }

        [Fact(Timeout = 5000)]
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

                    receiver.MessageReceived -= RecordMessageEvent;
                    receiver.MessageQueuedForReceive -= RecordMessageEvent;

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

        [Fact(Timeout = 5000)]
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

                    receiver.MessageReceived -= RecordMessageEvent;
                    receiver.MessageQueuedForReceive -= RecordMessageEvent2;

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

        public void Dispose()
        {
            if (lastCreatedSender != null)
                lastCreatedSender.Dispose();

            if (lastCreatedReceiver != null)
                lastCreatedReceiver.Dispose();
        }
    }
}