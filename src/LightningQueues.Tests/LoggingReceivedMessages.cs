using System;
using System.Linq;
using System.Threading;
using System.Transactions;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Tests.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class LoggingReceivedMessages
    {
        private QueueManager _sender;
        private QueueManager _receiver;
        private RecordingLogger _logger;

        [SetUp]
        public void Setup()
        {
            _logger = new RecordingLogger();
            _sender = ObjectMother.QueueManager();
            _sender.Start();
            _receiver = ObjectMother.QueueManager("test2", 23457, logger: _logger);
            _receiver.Start();
        }

        [Test]
        public void MessageQueuedForReceive_EventIsRaised()
        {
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

            Wait.Until(() => _logger.DebugMessages.OfType<MessageQueuedForReceive>().Any()).ShouldBeTrue();

            var log = _logger.DebugMessages.OfType<MessageQueuedForReceive>().First();
            log.Message.Queue.ShouldEqual("h");
        }

        [Test]
        public void MessageQueuedForReceive_EventIsRaised_DirectEnqueuing()
        {
            using (var tx = new TransactionScope())
            {
                _receiver.EnqueueDirectlyTo("h", null, new MessagePayload {Data = new byte[] {1, 2, 3}});
                tx.Complete();
            }
            Wait.Until(() => _logger.DebugMessages.OfType<MessageQueuedForReceive>().Any()).ShouldBeTrue();

            var log = _logger.DebugMessages.OfType<MessageQueuedForReceive>().First();
            "h".ShouldEqual(log.Message.Queue);
        }

        [Test]
        public void MessageQueuedForReceive_EventNotRaised_IfReceiveAborts()
        {
            ManualResetEvent wait = new ManualResetEvent(false);

            var sender = new FakeSender(ObjectMother.Logger())
            {
                Destination = new Endpoint("localhost", 23457),
                Messages = new[]
                {
                    new Message
                    {
                        Id =
                            new MessageId
                            {
                                MessageIdentifier = Guid.NewGuid(),
                                SourceInstanceId = Guid.NewGuid()
                            },
                        SentAt = DateTime.Now,
                        Queue = "h",
                        Data = new byte[] {1, 2, 4, 5}
                    }
                }
            };

            sender.SendCompleted += () => wait.Set();
            var logger = new RecordingLogger();
            sender.Send();
            wait.WaitOne(TimeSpan.FromSeconds(1));
            Wait.Until(() => logger.DebugMessages.OfType<MessageQueuedForReceive>().Any(),
                timeoutInMilliseconds: 1000)
                .ShouldBeFalse();

        }

        [Test]
        public void MessageReceived_EventIsRaised()
        {
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

            using (var tx = new TransactionScope())
            {
                _receiver.Receive("h");
                tx.Complete();
            }

            Wait.Until(() => _logger.DebugMessages.OfType<MessageReceived>().Any()).ShouldBeTrue();

            var log = _logger.DebugMessages.OfType<MessageReceived>().First();
            "h".ShouldEqual(log.Message.Queue);
        }

        [Test]
        public void MessageReceived_EventNotRaised_IfMessageNotReceived()
        {
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
            Wait.Until(() => _logger.DebugMessages.OfType<MessageReceived>().Any(), timeoutInMilliseconds: 1000)
                .ShouldBeFalse();
        }

        [Test]
        public void MessageReceived_and_MessageQueuedForReceive_events_raised_when_message_removed_and_moved()
        {
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

            using (var tx = new TransactionScope())
            {
                var message = _receiver.Receive("h");
                _receiver.MoveTo("b", message);
                tx.Complete();
            }

            Wait.Until(() => _logger.DebugMessages.OfType<MessageQueuedForReceive>().Count() == 2).ShouldBeTrue();

            var messageReceived = _logger.DebugMessages.OfType<MessageReceived>().First();
            "h".ShouldEqual(messageReceived.Message.Queue);
            messageReceived.Message.SubQueue.ShouldEqual("b");

            var messageQueuedForReceive = _logger.DebugMessages.OfType<MessageQueuedForReceive>().Last();

            "h".ShouldEqual(messageQueuedForReceive.Message.Queue);
            "b".ShouldEqual(messageQueuedForReceive.Message.SubQueue);
        }

        [Test]
        public void MessageReceived_and_MessageQueuedForReceive_events_raised_when_message_peeked_and_moved()
        {
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


            using (var tx = new TransactionScope())
            {
                var message = _receiver.Peek("h");
                _receiver.MoveTo("b", message);
                tx.Complete();
            }

            Wait.Until(() => _logger.DebugMessages.OfType<MessageQueuedForReceive>().Any()).ShouldBeTrue();

            var messageReceived = _logger.DebugMessages.OfType<MessageReceived>().Last();
            "h".ShouldEqual(messageReceived.Message.Queue);
            messageReceived.Message.SubQueue.ShouldEqual("b");

            var messageQueuedForReceive = _logger.DebugMessages.OfType<MessageQueuedForReceive>().Last();

            "h".ShouldEqual(messageQueuedForReceive.Message.Queue);
            "b".ShouldEqual(messageQueuedForReceive.Message.SubQueue);
        }

        [TearDown]
        public void TearDown()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }
    }
}