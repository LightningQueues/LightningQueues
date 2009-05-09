namespace Rhino.Queues.Tests
{
	using System;
	using System.IO;
	using System.Net;
	using System.Threading;
	using System.Transactions;
	using Queues.Protocol;
	using Xunit;

	public class Errors : IDisposable
	{
		 private readonly QueueManager sender, receiver;

        public Errors()
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
		public void Will_get_notified_when_failed_to_send_to_endpoint()
		{
			var wait = new ManualResetEvent(false);
			Endpoint endPointWeFailedToSendTo = null;
			sender.FailedToSendMessagesTo += endpoint =>
			{
				endPointWeFailedToSendTo = endpoint;
				wait.Set();
			};
			using(var tx = new TransactionScope())
			{

				sender.Send(new Uri("rhino.queues://255.255.255.255/hello/world"), new MessagePayload
				{
					Data = new byte[] {1}
				});
				tx.Complete();
			}

			wait.WaitOne();

			Assert.Equal("255.255.255.255",endPointWeFailedToSendTo.Host);
			Assert.Equal(2200, endPointWeFailedToSendTo.Port);
		}

		public void Dispose()
		{
			sender.Dispose();
			receiver.Dispose();
		}
	}
}