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
        private readonly QueueManager sender;

        public Errors()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            sender.Start();
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

        [Fact]
        public void Will_not_exceed_sending_thresholds()
        {
            var wait = new ManualResetEvent(false);
            int maxNumberOfConnecting = 0;
			sender.FailedToSendMessagesTo += endpoint =>
			{
			    maxNumberOfConnecting = Math.Max(maxNumberOfConnecting, sender.CurrentlyConnectingCount);
                if(endpoint.Host.Equals("foo50"))
				    wait.Set();
			};
			using(var tx = new TransactionScope())
			{
                for (int i = 0; i < 200; ++i)
                {
                    sender.Send(new Uri(string.Format("rhino.queues://foo{0}/hello/world", i)), new MessagePayload
                    {
                        Data = new byte[] {1}
                    });
                }
			    tx.Complete();
			}

			wait.WaitOne();
            Assert.True(maxNumberOfConnecting < 32);
        }

		public void Dispose()
		{
			sender.Dispose();
		}
	}
}