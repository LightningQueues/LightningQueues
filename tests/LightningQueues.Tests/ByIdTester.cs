using System;
using System.Transactions;
using LightningQueues.Model;
using Xunit;
using Should;

namespace LightningQueues.Tests
{
    public class ByIdTester : IDisposable
    {
        private QueueManager _queue;
        private QueueManager _receiver;
        private MessageId _messageId;

        public ByIdTester()
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

        [Fact(Skip="Not on mono")]
        public void retrieving_outgoing_message()
        {
            var message = _queue.GetMessageCurrentlySendingById(_messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        [Fact(Skip="Not on mono")]
        public void retrieving_outgoinghistory_message()
        {
            readMessage();

            var message = _queue.GetSentMessageById(_messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        [Fact(Skip="Not on mono")]
        public void retrieving_processed_message()
        {
            readMessage();

            var message = _receiver.GetProcessedMessageById("h", _messageId);
            new byte[] {1, 2, 4, 5}.ShouldEqual(message.Data);
        }

        public void Dispose()
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