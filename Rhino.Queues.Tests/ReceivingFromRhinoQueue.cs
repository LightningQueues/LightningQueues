using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Transactions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class ReceivingFromRhinoQueue : WithDebugging, IDisposable
    {
        private readonly QueueManager queueManager;

        public ReceivingFromRhinoQueue()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            queueManager.CreateQueues("h");
        }

        #region IDisposable Members

        public void Dispose()
        {
            queueManager.Dispose();
        }

        #endregion

        [Fact]
        public void CanReceiveFromQueue()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => null,
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (var tx = new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                Assert.Throws<TimeoutException>(() => queueManager.Receive("h", null, TimeSpan.Zero));

                tx.Complete();
            }
        }

		[Fact]
		public void WhenSendingDuplicateMessageTwiceWillGetItOnlyOnce()
		{
			var msg = new Message
			{
				Id = MessageId.GenerateRandom(),
				Queue = "h",
				Data = Encoding.Unicode.GetBytes("hello"),
				SentAt = DateTime.Now
			};
			for (int i = 0; i < 2; i++)
			{
				var wait = new ManualResetEvent(false);
				var sender = new Sender
				{
					Destination = new Endpoint("localhost", 23456),
					Failure = exception => Assert.False(true),
					Success = () => null,
					Messages = new[] { msg, },
				};
				sender.SendCompleted += () => wait.Set();
				sender.Send();
				wait.WaitOne();
			}

			using (var tx = new TransactionScope())
			{
				var message = queueManager.Receive("h", null);
				Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));

				tx.Complete();
			}

			using (var tx = new TransactionScope())
			{
				Assert.Throws<TimeoutException>(() => queueManager.Receive("h", null, TimeSpan.Zero));

				tx.Complete();
			}
		}

        [Fact]
        public void WhenRevertingTransactionMessageGoesBackToQueue()
        {
            new Sender
            {

                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => null,
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }

            using (new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
        }

        [Fact]
        public void CanLookupProcessedMessages()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => null,
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (var tx = new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            Thread.Sleep(500);

            var messages = queueManager.GetAllProcessedMessages("h");
            Assert.Equal(1, messages.Length);
            Assert.Equal("hello", Encoding.Unicode.GetString(messages[0].Data));
        }

        [Fact]
        public void CanPeekExistingMessages()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => null,
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using(new TransactionScope())
            {
                // force a wait until we receive the message
                queueManager.Receive("h", null);
            }

            var messages = queueManager.GetAllMessages("h", null);
            Assert.Equal(1, messages.Length);
            Assert.Equal("hello", Encoding.Unicode.GetString(messages[0].Data));
        }
    }
}
