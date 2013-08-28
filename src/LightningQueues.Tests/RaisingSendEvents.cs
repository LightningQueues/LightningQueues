using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class RaisingSendEvents
    {
        private const string TEST_QUEUE_1 = "testA.esent";
        private const string TEST_QUEUE_2 = "testB.esent";

        private MessageEventArgs messageEventArgs;

        public QueueManager SetupSender()
        {
            var sender = ObjectMother.QueueManager(TEST_QUEUE_1);
            sender.Start();
            messageEventArgs = null;
            return sender;
        }

        void RecordMessageEvent(object s, MessageEventArgs e)
        {
            messageEventArgs = e;
        }

        [Test]
        public void MessageQueuedForSend_EventIsRaised()
        {
            using(var sender = SetupSender())
            {
                sender.MessageQueuedForSend += RecordMessageEvent;

                using (var tx = new TransactionScope())
                {
                    sender.Send(
                        new Uri("lq.tcp://localhost:23999/h"),
                         new MessagePayload
                         {
                             Data = new byte[] { 1, 2, 4, 5 }
                         });

                    tx.Complete();
                }

                sender.MessageQueuedForSend -= RecordMessageEvent;
            }

            messageEventArgs.ShouldNotBeNull();
            "localhost".ShouldEqual(messageEventArgs.Endpoint.Host);
            23999.ShouldEqual(messageEventArgs.Endpoint.Port);
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
        public void MessageQueuedForSend_EventIsRaised_EvenIfTransactionFails()
        {
            using(var sender = SetupSender())
            {
                sender.MessageQueuedForSend += RecordMessageEvent;

                using (new TransactionScope())
                {
                    sender.Send(
                        new Uri("lq.tcp://localhost:23999/h"),
                        new MessagePayload
                        {
                            Data = new byte[] { 1, 2, 4, 5 }
                        });

                }

                sender.MessageQueuedForSend -= RecordMessageEvent;
            }

            messageEventArgs.ShouldNotBeNull();
            "localhost".ShouldEqual(messageEventArgs.Endpoint.Host);
            23999.ShouldEqual(messageEventArgs.Endpoint.Port);
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
        public void MessageSent_EventIsRaised()
        {
            using(var sender = SetupSender())
            {
                sender.MessageSent += RecordMessageEvent;

                using (var receiver = ObjectMother.QueueManager(TEST_QUEUE_2, 23457))
                {
                    receiver.Start();

                    using (var tx = new TransactionScope())
                    {
                        sender.Send(
                            new Uri("lq.tcp://localhost:23457/h"),
                            new MessagePayload
                                {
                                    Data = new byte[] {1, 2, 4, 5}
                                });

                        tx.Complete();
                    }
                    sender.WaitForAllMessagesToBeSent();
                }

                sender.MessageSent -= RecordMessageEvent;
            }

            messageEventArgs.ShouldNotBeNull();
            "localhost".ShouldEqual(messageEventArgs.Endpoint.Host);
            23457.ShouldEqual(messageEventArgs.Endpoint.Port);
            "h".ShouldEqual(messageEventArgs.Message.Queue);
        }

        [Test]
        public void MessageSent_EventNotRaised_IfNotSent()
        {
            using (var sender = SetupSender())
            {

                sender.MessageSent += RecordMessageEvent;

                using (var tx = new TransactionScope())
                {
                    sender.Send(
                        new Uri("lq.tcp://localhost:23999/h"),
                        new MessagePayload
                            {
                                Data = new byte[] {1, 2, 4, 5}
                            });

                    tx.Complete();
                }

                sender.MessageSent -= RecordMessageEvent;
            }

            messageEventArgs.ShouldBeNull();
        }

        [Test]
        public void MessageSent_EventNotRaised_IfReceiverReverts()
        {
            using (var sender = SetupSender())
            {
                sender.MessageSent += RecordMessageEvent;

                using (var receiver = new RevertingQueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2, null, ObjectMother.Logger()))
                {
                    receiver.CreateQueues("h");
                    receiver.Start();

                    using (var tx = new TransactionScope())
                    {
                        sender.Send(
                            new Uri("lq.tcp://localhost:23457/h"),
                            new MessagePayload
                                {
                                    Data = new byte[] {1, 2, 4, 5}
                                });

                        tx.Complete();
                    }

                    sender.MessageSent -= RecordMessageEvent;
                }
            }

            messageEventArgs.ShouldBeNull();
        }

        private class RevertingQueueManager : QueueManager
        {
            public RevertingQueueManager(IPEndPoint endpoint, string path, QueueManagerConfiguration configuration, ILogger logger)
                : base(endpoint, path, configuration, logger)
            {
            }

            protected override IMessageAcceptance AcceptMessages(Message[] msgs)
            {
                throw new Exception("Cannot accept messages.");
            }
        }
    }
}