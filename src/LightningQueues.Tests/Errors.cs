using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Transactions;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Logging;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class Errors
	{
        private QueueManager _sender;
        private RecordingLogger _logger;

        [SetUp]
        public void Setup()
        {
            _logger = new RecordingLogger();
            _sender = ObjectMother.QueueManager(logger:_logger);
            _sender.Start();
        }

		[Test]
		public void Will_get_notified_when_failed_to_send_to_endpoint()
		{
			using(var tx = new TransactionScope())
			{
				_sender.Send(new Uri("lq.tcp://255.255.255.255/hello/world"), new MessagePayload
				{
					Data = new byte[] {1}
				});
				tx.Complete();
			}

		    Wait.Until(() => _logger.InfoMessages.OfType<FailedToSend>().Any()).ShouldBeTrue();

		    var log = _logger.InfoMessages.OfType<FailedToSend>().First();
		    "255.255.255.255".ShouldEqual(log.Destination.Host);
		    2200.ShouldEqual(log.Destination.Port);
		}

        [Test]
        [Ignore("Needs to be its own test for sending choke")]
        public void Will_not_exceed_sending_thresholds()
        {
            var wait = new ManualResetEvent(false);
            int maxNumberOfConnecting = 0;
            //_sender.FailedToSendMessagesTo += endpoint =>
            //{
            //    maxNumberOfConnecting = Math.Max(maxNumberOfConnecting, _sender.SendingThrottle.CurrentlyConnectingCount);
            //    if(endpoint.Host.Equals("foo50"))
            //        wait.Set();
            //};
			using(var tx = new TransactionScope())
			{
                for (int i = 0; i < 200; ++i)
                {
                    _sender.Send(new Uri(string.Format("lq.tcp://foo{0}/hello/world", i)), new MessagePayload
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
			_sender.Dispose();
		}
	}
}