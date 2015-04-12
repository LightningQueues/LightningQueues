using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Should;
using LightningQueues.Exceptions;
using LightningQueues.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Protocol.Chunks;
using Xunit;
using LogManager = LightningQueues.Logging.LogManager;

namespace LightningQueues.Tests.Protocol
{
    public class ProtocolChunkTester
    {
        private ILogger _logger = LogManager.GetLogger<ProtocolChunkTester>();

        [Fact(Skip="Not on mono")]
        public void write_serialization_error()
        {
            var result = processChunk(new WriteSerializationError(_logger));
            result.ShouldEqual(ProtocolConstants.SerializationFailureBuffer);
        }

        [Fact(Skip="Not on mono")]
        public void write_revert()
        {
            var result = processChunk(new WriteRevert(_logger));
            result.ShouldEqual(ProtocolConstants.RevertBuffer);
        }

        [Fact(Skip="Not on mono")]
        public void write_received()
        {
            var result = processChunk(new WriteReceived(_logger));
            result.ShouldEqual(ProtocolConstants.RecievedBuffer);
        }

        [Fact(Skip="Not on mono")]
        public void write_processing_error()
        {
            var result = processChunk(new WriteProcessingError(_logger, ProtocolConstants.ProcessingFailureBuffer));
            result.ShouldEqual(ProtocolConstants.ProcessingFailureBuffer);
        }

        [Fact(Skip="Not on mono")]
        public void write_message()
        {
            var message = new byte[] { 1, 2, 5 };
            var result = processChunk(new WriteMessage(_logger, message));
            result.ShouldEqual(message);
        }

        [Fact(Skip="Not on mono")]
        public void write_length()
        {
            const int length = 5;
            var result = processChunk(new WriteLength(_logger, length));
            length.ShouldEqual(BitConverter.ToInt32(result, 0));
        }

        [Fact(Skip="Not on mono")]
        public void write_acknowledgement()
        {
            var result = processChunk(new WriteAcknowledgement(_logger));
            result.ShouldEqual(ProtocolConstants.AcknowledgedBuffer);
        }

        [Fact(Skip="Not on mono")]
        public void read_message()
        {
            var message = new Message();
            message.Data = new byte[] { 1, 2, 3, 4 };
            message.Id = MessageId.GenerateRandom();
            message.Headers.Add("fake", "fakevalue");
            message.Queue = "myqueue";
            message.SentAt = DateTime.Now;
            var messageBytes = new[] { message }.Serialize();

            var messages = getChunk<ReadMessage, Message[]>(new ReadMessage(_logger, messageBytes.Length), new MemoryStream(messageBytes));
            var afterSerialization = messages[0];
            afterSerialization.Data.ShouldEqual(message.Data);
            afterSerialization.Id.ShouldEqual(message.Id);
            afterSerialization.Queue.ShouldEqual(message.Queue);
            afterSerialization.SentAt.ShouldEqual(message.SentAt);
            afterSerialization.Headers.ShouldEqual(message.Headers);
        }

        [Fact(Skip="Not on mono")]
        public void read_message_fails_to_deserialize_throws()
        {
            var ms = new MemoryStream(Encoding.Unicode.GetBytes("Fail!"));
            expectException<SerializationException>(() => getChunk<ReadMessage, Message[]>(new ReadMessage(_logger, 3), ms));
        }

        [Fact(Skip="Not on mono")]
        public void read_acknowledgement()
        {
            var ms = new MemoryStream(ProtocolConstants.AcknowledgedBuffer);
            processChunk(new ReadAcknowledgement(_logger), ms);
            //nothing to assert, but should not throw or hang
        }

        [Fact(Skip="Not on mono")]
        public void read_acknowledgement_and_format_is_unexpected_should_throw()
        {
            var ms = new MemoryStream(Encoding.Unicode.GetBytes("Acknowledges"));
            processChunkWithExpectedErrors<ReadAcknowledgement, InvalidAcknowledgementException>(new ReadAcknowledgement(_logger), ms);
        }

        [Fact(Skip="Not on mono")]
        public void read_received()
        {
            var ms = new MemoryStream(ProtocolConstants.RecievedBuffer);
            processChunk(new ReadReceived(_logger), ms);
            //nothing to assert, but should not throw or hang
        }

        [Fact(Skip="Not on mono")]
        public void read_received_and_format_is_unexpected_should_throw()
        {
            var ms = new MemoryStream(Encoding.Unicode.GetBytes("Reciever"));
            processChunkWithExpectedErrors<ReadReceived, UnexpectedReceivedMessageFormatException>(new ReadReceived(_logger), ms);
        }

        [Fact(Skip="Not on mono")]
        public void read_received_but_queue_doesn_not_exist_chunk()
        {
            var ms = new MemoryStream(ProtocolConstants.QueueDoesNoExiststBuffer);
            processChunkWithExpectedErrors<ReadReceived, QueueDoesNotExistsException>(new ReadReceived(_logger), ms);
        }

        private byte[] processChunk<TChunk>(TChunk chunkWriter, MemoryStream ms = null) where TChunk : Chunk
        {
            ms = ms ?? new MemoryStream();
            var task = chunkWriter.ProcessAsync(ms);
            task.Wait();
            return ms.ToArray();
        }

        private TResult getChunk<TChunk, TResult>(TChunk chunk, MemoryStream ms) where TChunk : Chunk<TResult>
        {
            var task = chunk.GetAsync(ms);
            task.Wait();
            return task.Result;
        }

        private void processChunkWithExpectedErrors<TChunk, TException>(TChunk chunk, MemoryStream ms = null)
            where TChunk : Chunk
            where TException : Exception
        {
            expectException<TException>(() => processChunk(chunk, ms));
        }

        private void expectException<TException>(Action action) where TException : Exception
        {
            bool threwError = false;
            try
            {
                action();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.OfType<TException>().Any())
                    threwError = true;
            }
            catch (TException)
            {
                threwError = true;
            }
            threwError.ShouldBeTrue();
        }
    }
}