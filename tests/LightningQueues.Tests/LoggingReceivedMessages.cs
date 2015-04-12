using System;
using System.Linq;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Tests.Protocol;
using Xunit;

namespace LightningQueues.Tests
{
    public class LoggingReceivedMessages : IDisposable
    {
        private QueueManager _sender;
        private QueueManager _receiver;
        private RecordingLogger _logger;

        public LoggingReceivedMessages()
        {
            _logger = new RecordingLogger();
            _sender = ObjectMother.QueueManager();
            _sender.Start();
            _receiver = ObjectMother.QueueManager("test2", 23457, logger: _logger);
            _receiver.Start();
        }

        [Fact(Skip = "Not on mono")]
        public void MessageQueuedForReceive_IsLogged()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }

            Wait.Until(() => _logger.MessagesQueuedForReceive.Any()).ShouldBeTrue();

            var msg = _logger.MessagesQueuedForReceive.First();
            msg.Queue.ShouldEqual("h");
        }

        [Fact(Skip = "Not on mono")]
        public void MessageQueuedForReceive_IsLogged_DirectEnqueuing()
        {
            using (var tx = new TransactionScope())
            {
                _receiver.EnqueueDirectlyTo("h", null, new MessagePayload { Data = new byte[] { 1, 2, 3 } });
                tx.Complete();
            }
            Wait.Until(() => _logger.MessagesQueuedForReceive.Any()).ShouldBeTrue();

            var msg = _logger.MessagesQueuedForReceive.First();
            msg.Queue.ShouldEqual("h");
        }

        [Fact(Skip = "Not on mono")]
        public void MessageQueuedForReceive_IsNotLogged_IfReceiveAborts()
        {
            ManualResetEvent wait = new ManualResetEvent(false);

            var sender = new FakeSender
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
            Wait.Until(() => logger.MessagesQueuedForReceive.Any(),
                timeoutInMilliseconds: 1000)
                .ShouldBeFalse();

        }

        [Fact(Skip = "Not on mono")]
        public void MessageReceived_IsLogged()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }
            _sender.WaitForAllMessagesToBeSent();

            using (var tx = new TransactionScope())
            {
                _receiver.Receive("h");
                tx.Complete();
            }

            Wait.Until(() => _logger.ReceivedMessages.Any()).ShouldBeTrue();

            var message = _logger.ReceivedMessages.First();
            "h".ShouldEqual(message.Queue);
        }

        [Fact(Skip = "Not on mono")]
        public void MessageReceived_IsNotLogged_IfMessageNotReceived()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });

                tx.Complete();
            }
            Wait.Until(() => _logger.DebugMessages.Any(), timeoutInMilliseconds: 1000)
                .ShouldBeFalse();
        }

        [Fact(Skip = "Not on mono")]
        public void MessageReceived_and_MessageQueuedForReceive_logged_when_message_removed_and_moved()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
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

            Wait.Until(() => _logger.MessagesQueuedForReceive.Count() == 2).ShouldBeTrue();

            var messageReceived = _logger.ReceivedMessages.First();
            "h".ShouldEqual(messageReceived.Queue);
            messageReceived.SubQueue.ShouldEqual("b");

            var messageQueuedForReceive = _logger.MessagesQueuedForReceive.Last();

            "h".ShouldEqual(messageQueuedForReceive.Queue);
            "b".ShouldEqual(messageQueuedForReceive.SubQueue);
        }

        [Fact(Skip = "Not on mono")]
        public void MessageReceived_and_MessageQueuedForReceive_logged_when_message_peeked_and_moved()
        {
            using (var tx = new TransactionScope())
            {
                _sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
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

            Wait.Until(() => _logger.MessagesQueuedForReceive.Any()).ShouldBeTrue();

            var messageReceived = _logger.ReceivedMessages.Last();
            "h".ShouldEqual(messageReceived.Queue);
            messageReceived.SubQueue.ShouldEqual("b");

            var messageQueuedForReceive = _logger.MessagesQueuedForReceive.Last();

            "h".ShouldEqual(messageQueuedForReceive.Queue);
            "b".ShouldEqual(messageQueuedForReceive.SubQueue);
        }

        public void Dispose()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }
    }
}