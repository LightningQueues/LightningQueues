using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class Errors
	{
        private QueueManager sender;

        [SetUp]
        public void Setup()
        {
            sender = ObjectMother.QueueManager();
            sender.Start();
        }

		[Test]
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
				sender.Send(new Uri("lq.tcp://255.255.255.255/hello/world"), new MessagePayload
				{
					Data = new byte[] {1}
				});
				tx.Complete();
			}

			wait.WaitOne(TimeSpan.FromSeconds(10));

		    "255.255.255.255".ShouldEqual(endPointWeFailedToSendTo.Host);
		    2200.ShouldEqual(endPointWeFailedToSendTo.Port);
		}

        [Test]
        public void Will_not_exceed_sending_thresholds()
        {
            var wait = new ManualResetEvent(false);
            int maxNumberOfConnecting = 0;
			sender.FailedToSendMessagesTo += endpoint =>
			{
			    maxNumberOfConnecting = Math.Max(maxNumberOfConnecting, sender.SendingThrottle.CurrentlyConnectingCount);
                if(endpoint.Host.Equals("foo50"))
				    wait.Set();
			};
			using(var tx = new TransactionScope())
			{
                for (int i = 0; i < 200; ++i)
                {
                    sender.Send(new Uri(string.Format("lq.tcp://foo{0}/hello/world", i)), new MessagePayload
                    {
                        Data = new byte[] {1}
                    });
                }
			    tx.Complete();
			}

			wait.WaitOne(TimeSpan.FromSeconds(10));
            Assert.True(maxNumberOfConnecting < 32);
        }

        [TearDown]
		public void TearDown()
		{
			sender.Dispose();
		}
	}
}