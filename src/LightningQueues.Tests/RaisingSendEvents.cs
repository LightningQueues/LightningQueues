using System;
using System.Linq;
using System.Net;
using System.Transactions;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Logging;
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
        private QueueManager _sender;
        private RecordingLogger _logger;

        [SetUp]
        public void Setup()
        {
            _logger = new RecordingLogger();
            _sender = ObjectMother.QueueManager(TEST_QUEUE_1, logger:_logger);
            _sender.Start();
        }

        [TearDown]
        public void Teardown()
        {
            _sender.Dispose();
        }

        [Test]
        public void MessageQueuedForSend_EventIsRaised()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23999/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });

                tx.Complete();
            }

            var log = _logger.DebugMessages.OfType<MessageQueuedForSend>().FirstOrDefault();

            log.ShouldNotBeNull();
            "localhost".ShouldEqual(log.Destination.Host);
            23999.ShouldEqual(log.Destination.Port);
            "h".ShouldEqual(log.Message.Queue);
        }

        [Test]
        public void MessageQueuedForSend_EventIsRaised_EvenIfTransactionFails()
        {
            using (new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23999/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });
            }

            var log = _logger.DebugMessages.OfType<MessageQueuedForSend>().FirstOrDefault();
            log.ShouldNotBeNull();
            "localhost".ShouldEqual(log.Destination.Host);
            23999.ShouldEqual(log.Destination.Port);
            "h".ShouldEqual(log.Message.Queue);
        }

        [Test]
        public void MessageSent_EventIsRaised()
        {
            using (var receiver = ObjectMother.QueueManager(TEST_QUEUE_2, 23457))
            {
                receiver.Start();

                using (var tx = new TransactionScope())
                {
                    _sender.Send(
                        new Uri("lq.tcp://localhost:23457/h"),
                        new MessagePayload
                        {
                            Data = new byte[] {1, 2, 4, 5}
                        });

                    tx.Complete();
                }
                _sender.WaitForAllMessagesToBeSent();
            }

            var log = _logger.DebugMessages.OfType<MessagesSent>().FirstOrDefault();
            log.ShouldNotBeNull();
            "localhost".ShouldEqual(log.Destination.Host);
            23457.ShouldEqual(log.Destination.Port);
            "h".ShouldEqual(log.Messages[0].Queue);
        }

        [Test]
        public void MessageSent_EventNotRaised_IfNotSent()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23999/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });

                tx.Complete();
            }

            var log = _logger.DebugMessages.OfType<MessagesSent>().FirstOrDefault();
            log.ShouldBeNull();
        }

        [Test]
        public void MessageSent_EventNotRaised_IfReceiverReverts()
        {
            using (var receiver = new RevertingQueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2, new QueueManagerConfiguration(), ObjectMother.Logger()))
            {
                receiver.CreateQueues("h");
                receiver.Start();

                using (var tx = new TransactionScope())
                {
                    _sender.Send(
                        new Uri("lq.tcp://localhost:23457/h"),
                        new MessagePayload
                        {
                            Data = new byte[] {1, 2, 4, 5}
                        });

                    tx.Complete();
                }
            }

            var log = _logger.DebugMessages.OfType<MessagesSent>().FirstOrDefault();
            log.ShouldBeNull();
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