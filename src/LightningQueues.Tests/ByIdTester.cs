using System;
using System.Transactions;
using FubuTestingSupport;
using LightningQueues.Model;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class ByIdTester
    {
        private QueueManager _queue;
        private QueueManager _receiver;
        private MessageId _messageId;

        [SetUp]
        public void Setup()
        {
            _queue = ObjectMother.QueueManager();
            _queue.Start();

            _receiver = ObjectMother.QueueManager("test2", 23457);
            using (var tx = new TransactionScope())
            {
                _messageId = _queue.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });

                tx.Complete();
            }
        }

        [Test]
        public void retrieving_outgoing_message()
        {
            var message = _queue.GetMessageCurrentlySendingById(_messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        [Test]
        public void retrieving_outgoinghistory_message()
        {
            readMessage();

            var message = _queue.GetSentMessageById(_messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        [Test]
        public void retrieving_processed_message()
        {
            readMessage();

            var message = _receiver.GetProcessedMessageById("h", _messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        [TearDown]
        public void Teardown()
        {
            _queue.Dispose();
            _receiver.Dispose();
        }

        private void readMessage()
        {
            _receiver.Start();
            using (var tx = new TransactionScope())
            {
                _receiver.Receive("h", null);
                tx.Complete();
            }
        }
    }
}