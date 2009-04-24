namespace Rhino.Queues.Tests.FromUsers
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Net;
	using System.Text;
	using System.Threading;
	using System.Transactions;
	using Model;
	using Protocol;
	using Xunit;

	public class FromRene : WithDebugging, IDisposable
	{
		readonly QueueManager receiver;
		private volatile bool keepRunning = true;
		private readonly List<string> msgs = new List<string>();

		public FromRene()
		{
			if (Directory.Exists("receiver.esent"))
				Directory.Delete("receiver.esent", true);
			if (Directory.Exists("sender.esent"))
				Directory.Delete("sender.esent", true);

			using (var tx = new TransactionScope())
			{
				receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 4545), "receiver.esent");

				receiver.CreateQueues("uno");
				tx.Complete();
			}
		}

		public void Sender(int count)
		{
			using (var sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 4546), "sender.esent"))
			{
				using (var tx = new TransactionScope())
				{
					sender.Send(new Uri("rhino.queues://localhost:4545/uno"),
					            new MessagePayload
					            {
					            	Data = Encoding.ASCII.GetBytes("Message "+count)
					            }
						);
					tx.Complete();
				}
				sender.WaitForAllMessagesToBeSent();
			}
		}

		public void Receiver(object ignored)
		{
			while (keepRunning)
			{
				using (var tx = new TransactionScope())
				{
					Message msg;
					try
					{
						msg = receiver.Receive("uno", null, new TimeSpan(0, 0, 10));
					}
					catch (TimeoutException)
					{
						continue;
					}
					catch(ObjectDisposedException)
					{
						continue;
					}
					lock (msgs)
					{
						msgs.Add(Encoding.ASCII.GetString(msg.Data));
						Console.WriteLine(msgs.Count);
					}
					tx.Complete();
				}
			}
		}

		[Fact]
		public void ShouldOnlyGetTwoItems()
		{
			ThreadPool.QueueUserWorkItem(Receiver);

			Sender(4);

			Sender(5);

			while(true)
			{
				lock (msgs)
				{
					if (msgs.Count>1)
						break;
				}
				Thread.Sleep(100);
			}
			Thread.Sleep(2000);//let it try to do something in addition to that
			receiver.Dispose();
			keepRunning = false;

			Assert.Equal(2, msgs.Count);
			Assert.Equal("Message 4", msgs[0]);
			Assert.Equal("Message 5", msgs[1]);
		}

		public void Dispose()
		{
			receiver.Dispose();
		}
	}
}
