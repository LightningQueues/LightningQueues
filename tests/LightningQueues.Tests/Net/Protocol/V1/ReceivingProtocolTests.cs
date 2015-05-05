using Xunit;
using Should;
using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using System.Threading.Tasks;
using LightningQueues.Storage;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Protocol;

namespace LightningQueues.Tests.Net.Protocol.V1
{
    public class ReceivingProtocolTests
    {
        readonly RecordingLogger _logger;
        readonly ReceivingProtocol _protocol;

        public ReceivingProtocolTests()
        {
            _logger = new RecordingLogger();
            _protocol = new ReceivingProtocol(new NoPersistenceMessageRepository(), _logger);
        }

        [Fact]
        public void client_sending_negative_length_wont_produce_next_length_item()
        {
            using (var ms = new MemoryStream())
            {
                ms.Write(BitConverter.GetBytes(-2), 0, 4);
                ms.Position = 0;
                using (_protocol.LengthChunk(ms).Subscribe(x => true.ShouldBeFalse()))
                {
                    //Need to evaluate a new assertion library.
                    //_logger.DebugMessages.ShouldContain("Read in length value of -2");
                }
            }
        }

        [Fact]
        public void handling_valid_length()
        {
            var length = 5;
            using (var ms = new MemoryStream())
            {
                ms.Write(BitConverter.GetBytes(length), 0, 4);
                ms.Position = 0;
                using (_protocol.LengthChunk(ms).SubscribeOn(Scheduler.CurrentThread)
                      .Subscribe(x => x.ShouldEqual(length)))
                {
                }
            }
        }

        [Fact]
        public void sending_shorter_length_than_payload_length()
        {
            runLengthTest(-2);
        }

        [Fact]
        public void sending_longer_length_than_payload_length()
        {
            runLengthTest(5);
        }

        private void runLengthTest(int differenceFromActualLength)
        {
            var message = new IncomingMessage();
            message.Id = MessageId.GenerateRandom();
            message.Data = System.Text.Encoding.UTF8.GetBytes("hello");
            message.Queue = "test";
            var bytes = new[] { message }.Serialize();
            using (var ms = new MemoryStream())
            {
                ms.Write(BitConverter.GetBytes(bytes.Length + differenceFromActualLength), 0, 4);
                ms.Write(bytes, 0, bytes.Length);
                ms.Position = 0;
                using (_protocol.MessagesChunk(ms, bytes.Length).SubscribeOn(Scheduler.CurrentThread)
                      .Subscribe(x => true.ShouldBeFalse()))
                {
                    _logger.DebugMessages.Any(x => x.StartsWith("Error deserializing messages")).ShouldBeTrue();
                }
            }
        }

        [Fact]
        public async Task storing_to_a_queue_that_doesnt_exist()
        {
            byte[] errorBytes = null;
            var protocol = new ReceivingProtocol(new ThrowingMessageRepository<QueueDoesNotExistException>(), _logger);
            using (var ms = new MemoryStream())
            {
                try
                {
                    await protocol.StoreMessages(ms, null);
                }
                catch (QueueDoesNotExistException)
                {
                    ms.Position = 0;
                    errorBytes = await ms.ReadBytesAsync(Constants.QueueDoesNotExistBuffer.Length);
                }
            }
            Constants.QueueDoesNotExistBuffer.ShouldEqual(errorBytes);
        }

        [Fact]
        public void sending_to_a_queue_that_doesnt_exist()
        {
            var protocol = new ReceivingProtocol(new ThrowingMessageRepository<QueueDoesNotExistException>(), _logger);
            var message = new IncomingMessage();
            message.Id = MessageId.GenerateRandom();
            message.Data = System.Text.Encoding.UTF8.GetBytes("hello");
            message.Queue = "test";
            var bytes = new[] { message }.Serialize();
            using (var ms = new MemoryStream())
            {
                ms.Write(BitConverter.GetBytes(bytes.Length), 0, 4);
                ms.Write(bytes, 0, bytes.Length);
                ms.Position = 0;
                using (protocol.ReceiveStream(Observable.Return(ms)).SubscribeOn(Scheduler.CurrentThread)
                       .Subscribe(x => true.ShouldBeFalse()))
                {
                }
            }
        }
    }
}