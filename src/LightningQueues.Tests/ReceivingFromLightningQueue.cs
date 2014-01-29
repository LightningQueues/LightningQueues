using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class ReceivingFromLightningQueue
    {
        private QueueManager queueManager;

        [SetUp]
        public void SetUp()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
        }

        [TearDown]
        public void Teardown()
        {
            queueManager.Dispose();
        }

        [Test]
        public void CanReceiveFromQueue()
        {
            new Sender(ObjectMother.Logger())
            {
                Destination = new Endpoint("localhost", 23456),
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
            }.Send().Wait();

            using (var tx = new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                Assert.Throws<TimeoutException>(() => queueManager.Receive("h", null, TimeSpan.Zero));

                tx.Complete();
            }
        }

		[Test]
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
				var sender = new Sender(ObjectMother.Logger())
				{
					Destination = new Endpoint("localhost", 23456),
					Messages = new[] { msg, },
				};
			    try
			    {
			        sender.Send().Wait();
			    }
			    catch (Exception)
			    {
			        //don't care if the sender throws on 2nd round
			    }
			}

			using (var tx = new TransactionScope())
			{
				var message = queueManager.Receive("h", null);
				"hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));

				tx.Complete();
			}

			using (var tx = new TransactionScope())
			{
				Assert.Throws<TimeoutException>(() => queueManager.Receive("h", null, TimeSpan.Zero));

				tx.Complete();
			}
		}

        [Test]
        public void WhenRevertingTransactionMessageGoesBackToQueue()
        {
            new Sender(ObjectMother.Logger())
            {

                Destination = new Endpoint("localhost", 23456),
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
            }.Send().Wait();

            using (new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));
            }

            using (new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));
            }
        }

        [Test]
        public void CanLookupProcessedMessages()
        {
            new Sender(ObjectMother.Logger())
            {
                Destination = new Endpoint("localhost", 23456),
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
            }.Send().Wait();

            using (var tx = new TransactionScope())
            {
                var message = queueManager.Receive("h", null);
                "hello".ShouldEqual(Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            var messages = queueManager.GetAllProcessedMessages("h");
            1.ShouldEqual(messages.Length);
            "hello".ShouldEqual(Encoding.Unicode.GetString(messages[0].Data));
        }

        [Test]
        public void CanPeekExistingMessages()
        {
            new Sender(ObjectMother.Logger())
            {
                Destination = new Endpoint("localhost", 23456),
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
            }.Send().Wait();

            using(new TransactionScope())
            {
                // force a wait until we receive the message
                queueManager.Receive("h", null);
            }

            var messages = queueManager.GetAllMessages("h", null);
            1.ShouldEqual(messages.Length);
            "hello".ShouldEqual(Encoding.Unicode.GetString(messages[0].Data));
        }
    }
}
