using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using Should;
using Xunit;

namespace LightningQueues.Tests
{
    public class SendingToLightningQueue : IDisposable
    {
        private QueueManager sender, receiver;

        public SendingToLightningQueue()
        {
            sender = ObjectMother.QueueManager();
            sender.Start();

            receiver = ObjectMother.QueueManager("test2", 23457);
            receiver.CreateQueues("a");
            receiver.Start();
        }

        [Fact(Skip = "Not on mono")]
        public void CanSendToQueue()
        {
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

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);

                new byte[] { 1, 2, 4, 5 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        [Fact(Skip = "Not on mono")]
        public void SendingTwoMessages_OneOfWhichToUnknownQueue_WillStillWork()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                sender.Send(
                    new Uri("lq.tcp://localhost:23457/I_dont_exists"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null, TimeSpan.FromSeconds(5));

                new byte[] { 1, 2, 4, 5 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        [Fact(Skip = "Not on mono")]
        public void CanSendHeaders()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 },
                         Headers =
                             {
                                 {"id","6"},
                                 {"date","2009-01-10"}
                             }
                     });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);

                "6".ShouldEqual(message.Headers["id"]);
                "2009-01-10".ShouldEqual(message.Headers["date"]);

                tx.Complete();
            }
        }

        [Fact(Skip = "Not on mono")]
        public void CanLookAtSentMessages()
        {
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

            using (var tx = new TransactionScope())
            {
                receiver.Receive("h", null);
                tx.Complete();
            }

            var messages = sender.GetAllSentMessages();
            1.ShouldEqual(messages.Length);
            new byte[] { 1, 2, 4, 5 }.ShouldEqual(messages[0].Data);
        }

        [Fact(Skip = "Not on mono")]
        public void CanLookAtMessagesCurrentlySending()
        {
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

        [Fact(Skip = "Not on mono")]
        public void WillNotSendIfTxIsNotCommitted()
        {
            using (new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });
            }

            using (var tx = new TransactionScope())
            {
                Assert.Throws<TimeoutException>(() => receiver.Receive("h", "subqueue", TimeSpan.FromSeconds(1)));

                tx.Complete();
            }
        }

        [Fact(Skip = "Not on mono")]
        public void CanSendSeveralMessagesToQueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 4, 5, 6, 7 }
                    });
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            var messages = new List<byte[]>();
            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);
                messages.Add(message.Data);

                var message2 = receiver.Receive("h", null);
                messages.Add(message2.Data);

                var message3 = receiver.Receive("h", null);
                messages.Add(message3.Data);

                tx.Complete();
            }
            messages.Any(x => x.SequenceEqual(new byte[] { 1, 2, 4, 5 })).ShouldBeTrue();
            messages.Any(x => x.SequenceEqual(new byte[] { 4, 5, 6, 7 })).ShouldBeTrue();
            messages.Any(x => x.SequenceEqual(new byte[] { 6, 7, 8, 9 })).ShouldBeTrue();
        }

        [Fact(Skip = "Not on mono")]
        public void CanSendMessagesToSeveralQueues()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/a"),
                    new MessagePayload
                    {
                        Data = new byte[] { 4, 5, 6, 7 }
                    });
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);
                new byte[] { 1, 2, 4, 5 }.ShouldEqual(message.Data);

                message = receiver.Receive("h", null);
                new byte[] { 6, 7, 8, 9 }.ShouldEqual(message.Data);

                message = receiver.Receive("a", null);
                new byte[] { 4, 5, 6, 7 }.ShouldEqual(message.Data);

                tx.Complete();
            }
        }

        public void Dispose()
        {
            sender.Dispose();
            receiver.Dispose();
        }
    }
}