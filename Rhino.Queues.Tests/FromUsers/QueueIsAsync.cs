namespace Rhino.Queues.Tests.FromUsers
{
	using System;
	using System.IO;
	using System.Net;
	using System.Text;
	using System.Threading;
	using System.Transactions;
	using Model;
	using Protocol;
	using Queues.Protocol;
	using Xunit;

	public class QueueIsAsync : WithDebugging, IDisposable
	{
		private readonly QueueManager queueManager;

		public QueueIsAsync()
		{
			if (Directory.Exists("test.esent"))
				Directory.Delete("test.esent", true);

			queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
			queueManager.CreateQueues("h");
		}

		[Fact]
		public void CanReceiveFromQueue()
		{
			for (int i = 0; i < 2; i++)
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

				Assert.NotNull(message);

				tx.Complete();
			}
			longTx.Set();
			wait.WaitOne();
		}

		public void Dispose()
		{
			queueManager.Dispose();
		}
	}
}