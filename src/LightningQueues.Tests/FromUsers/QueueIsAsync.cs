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

namespace LightningQueues.Tests.FromUsers
{
    [TestFixture]
    public class QueueIsAsync
	{
		private QueueManager queueManager;

        [SetUp]
		public void Setup()
		{
            queueManager = ObjectMother.QueueManager();
            queueManager.Start();
		}

		[Test]
		public void CanReceiveFromQueue()
		{
			for (int i = 0; i < 2; i++)
			{
				new Sender()
				{
					Destination = new Endpoint("localhost", 23456),
					Messages = new[]
					{
						new Message
						{
							Id = MessageId.GenerateRandom(),
							Queue = "h",
							Data = Encoding.Unicode.GetBytes("hello-" + i),
							SentAt = DateTime.Now
						},
					}
				}.Send();
			}
			var longTx = new ManualResetEvent(false);
			var wait = new ManualResetEvent(false);
			ThreadPool.QueueUserWorkItem(state =>
			{
				using (var tx = new TransactionScope())
				{
					queueManager.Receive("h", null);

					longTx.WaitOne();

					tx.Complete();
				}
				wait.Set();
			});

			using (var tx = new TransactionScope())
			{
				var message = queueManager.Receive("h", null);

			    message.ShouldNotBeNull();

				tx.Complete();
			}
			longTx.Set();
			wait.WaitOne();
		}

        [TearDown]
		public void Teardown()
		{
			queueManager.Dispose();
		}
	}
}