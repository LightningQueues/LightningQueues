using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using LightningQueues.Logging;

namespace LightningQueues.Net.Protocol.V1
{
    public class SendingProtocol : ISendingProtocol
    {
        readonly ILogger _logger;

        public SendingProtocol(ILogger logger)
        {
            _logger = logger;
        }

        public IObservable<Message> SendStream(IObservable<OutgoingMessageBatch> observableMessages)
        {
            return from tuple in SerializeOutgoing(observableMessages)
                                    .Do(x => WriteLength(x.Item2.Stream, x.Item1.Length))
                                    .Do(x => WriteMessages(x.Item2.Stream, x.Item1))
                   from received in ReadReceived(tuple.Item2.Stream).ToObservable().Where(x => x)
                                    .Do(x => WriteAcknowledge(tuple.Item2.Stream))
                   from m in tuple.Item2.Messages
                   select m;
        }

        public async Task WriteLength(Stream stream, int length)
        {
            _logger.Debug($"Writing length of {length}");
            var bufferLenInBytes = BitConverter.GetBytes(length);
            await stream.WriteAsync(bufferLenInBytes, 0, bufferLenInBytes.Length);
        }

        public async Task WriteMessages(Stream stream, byte[] messageBytes)
        {
            _logger.Debug("Starting to send.");
            await stream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false);
            _logger.Debug("Finished sending messages");
        }

        public async Task<bool> ReadReceived(Stream stream)
        {
            _logger.Debug("Waiting for receive buffer");
            var receivedBuffer = await stream.ReadBytesAsync(Constants.ReceivedBuffer.Length);
            if (receivedBuffer.SequenceEqual(Constants.ReceivedBuffer))
            {
                return true;
            }
            _logger.Debug("Received something different than received buffer");
            return false;
        }

        public async Task WriteAcknowledge(Stream stream)
        {
            _logger.Debug("Sending acknowledgement");
            await stream.WriteAsync(Constants.AcknowledgedBuffer, 0, Constants.AcknowledgedBuffer.Length);
        }

        public IObservable<Tuple<byte[], OutgoingMessageBatch>> SerializeOutgoing(IObservable<OutgoingMessageBatch> outgoing)
        {
            _logger.Debug("Serializing outgoing");
            return from message in outgoing
                   select Tuple.Create(message.Messages.Serialize(), message);
        }
    }
}