using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Tests.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class RaisingReceivedEvents
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

            lastCreatedSender = ObjectMother.QueueManager(TEST_QUEUE_1);
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

            lastCreatedReceiver = ObjectMother.QueueManager(TEST_QUEUE_2, 23457);
            lastCreatedReceiver.CreateQueues("b");
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

        [Test]
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
                            new Uri("lq.tcp://localhost:23457/h"),
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

            messageEventArgs.ShouldNotBeNull();
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
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

            messageEventArgs.ShouldNotBeNull();
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
        public void MessageQueuedForReceive_EventNotRaised_IfReceiveAborts()
        {
            ManualResetEvent wait = new ManualResetEvent(false);

            using (var sender = new FakeSender(ObjectMother.Logger())
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

        [Test]
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
                            new Uri("lq.tcp://localhost:23457/h"),
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
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
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
                            new Uri("lq.tcp://localhost:23457/h"),
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

            messageEventArgs.ShouldBeNull();
        }

        [Test]
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
                            new Uri("lq.tcp://localhost:23457/h"),
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

                    1.ShouldEqual(messageEventCount);
                    messageEventArgs.ShouldNotBeNull();
                    "h".ShouldEqual(messageEventArgs.Message.Queue);
                    messageEventArgs.Message.SubQueue.ShouldBeNull();

                    1.ShouldEqual(messageEventCount2);
                    messageEventArgs2.ShouldNotBeNull();
                    "h".ShouldEqual(messageEventArgs2.Message.Queue);
                    "b".ShouldEqual(messageEventArgs2.Message.SubQueue);
                }
            }
        }

        [Test]
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
                            new Uri("lq.tcp://localhost:23457/h"),
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

                    1.ShouldEqual(messageEventCount);
                    messageEventArgs.ShouldNotBeNull();
                    "h".ShouldEqual(messageEventArgs.Message.Queue);
                    messageEventArgs.Message.SubQueue.ShouldBeNull();

                    1.ShouldEqual(messageEventCount2);
                    messageEventArgs2.ShouldNotBeNull();
                    "h".ShouldEqual(messageEventArgs2.Message.Queue);
                    "b".ShouldEqual(messageEventArgs2.Message.SubQueue);
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (lastCreatedSender != null)
                lastCreatedSender.Dispose();

            if (lastCreatedReceiver != null)
                lastCreatedReceiver.Dispose();
        }
    }
}