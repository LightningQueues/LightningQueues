using System;
using System.IO;
using System.Net;
using System.Transactions;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class SendingToRhinoQueue : WithDebugging, IDisposable
    {
        private readonly QueueManager sender, receiver;

        public SendingToRhinoQueue()
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
        public void CanSendToQueue()
        {
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

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);

                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                tx.Complete();
            }
        }

		[Fact]
		public void SendingTwoMessages_OneOfWhichToUnknownQueue_WillStillWork()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h"),
					 new MessagePayload
					 {
						 Data = new byte[] { 1, 2, 4, 5 }
					 });

				sender.Send(
					new Uri("rhino.queues://localhost:23457/I_dont_exists"),
					 new MessagePayload
					 {
						 Data = new byte[] { 1, 2, 4, 5 }
					 });

				tx.Complete();
			}

			using (var tx = new TransactionScope())
			{
				var message = receiver.Receive("h", null, TimeSpan.FromSeconds(5));

				Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

				tx.Complete();
			}
		}

        [Fact]
        public void CanSendHeaders()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
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

                Assert.Equal("6", message.Headers["id"]);
                Assert.Equal("2009-01-10", message.Headers["date"]);

                tx.Complete();
            }
        }

        [Fact]
        public void CanLookAtSentMessages()
        {
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

            using (var tx = new TransactionScope())
            {
                receiver.Receive("h", null);
                tx.Complete();
            }

            var messages = sender.GetAllSentMessages();
            Assert.Equal(1, messages.Length);
            Assert.Equal(new byte[] {1, 2, 4, 5}, messages[0].Data);
        }

        [Fact]
        public void CanLookAtMessagesCurrentlySending()
        {
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
        public void WillNotSendIfTxIsNotCommitted()
        {
            using (new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
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

        [Fact]
        public void CanSendSeveralMessagesToQueue()
        {
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
                        Data = new byte[] { 4, 5, 6, 7 }
                    });
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);
                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                message = receiver.Receive("h", null);
                Assert.Equal(new byte[] { 4, 5, 6, 7 }, message.Data);

                message = receiver.Receive("h", null);
                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
        public void CanSendMessagesToSeveralQueues()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 1, 2, 4, 5 }
                    });
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/a"),
                    new MessagePayload
                    {
                        Data = new byte[] { 4, 5, 6, 7 }
                    });
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] { 6, 7, 8, 9 }
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", null);
                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                message = receiver.Receive("h", null);
                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                message = receiver.Receive("a", null);
                Assert.Equal(new byte[] { 4, 5, 6, 7 }, message.Data);

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