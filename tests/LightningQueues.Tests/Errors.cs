using System;
using System.Linq;
using System.Transactions;
using FubuTestingSupport;
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

		    Wait.Until(() => _logger.SendFailures.Any()).ShouldBeTrue();

		    var log = _logger.SendFailures.First();
		    "255.255.255.255".ShouldEqual(log.Destination.Host);
		    2200.ShouldEqual(log.Destination.Port);
		}

        [TearDown]
		public void TearDown()
		{
			_sender.Dispose();
		}
	}
}