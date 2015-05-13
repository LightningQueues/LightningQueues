using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Reactive;
using System.Reactive.Linq;
using LightningQueues.Logging;

namespace LightningQueues.Net.Protocol.V1
{
    public interface ISendingProtocol
    {
        IObservable<IncomingMessage> SendStream(IObservable<OutgoingMessageBatch> messages);
    }

    public class OutgoingMessageBatch
    {
        public Stream Stream { get; set; }
        public IncomingMessage[] Messages { get; set; }
    }

    public class SendingProtocol : ISendingProtocol
    {
        readonly ILogger _logger;

        public SendingProtocol(ILogger logger)
        {
            _logger = logger;
        }

        public IObservable<IncomingMessage> SendStream(IObservable<OutgoingMessageBatch> observableMessages)
        {
            return from messageParts in SerializeOutgoing(observableMessages)
                   from _ in WriteLength(messageParts.Item2.Stream, messageParts.Item1.Length)
                   from messages in WriteMessages(messageParts.Item2.Stream, messageParts.Item1, messageParts.Item2.Messages).Do(x => _logger.Debug("Wrote messages"))
                   from __ in ReceiveAndAcknowledge(messageParts.Item2.Stream).Do(x => _logger.Debug("Received ack"))
                   from m in messages
                   select m;
        }

        public IObservable<Unit> WriteLength(Stream stream, int length)
        {
            _logger.Debug($"Writing length of {length}");
            var bufferLenInBytes = BitConverter.GetBytes(length);
            return Observable.FromAsync(() => stream.WriteAsync(bufferLenInBytes, 0, bufferLenInBytes.Length));
        }

        public IObservable<IncomingMessage[]> WriteMessages(Stream stream, byte[] messageBytes, IncomingMessage[] original)
        {
            _logger.Debug($"Writing actual length of {messageBytes.Length}");
            return Observable.FromAsync(async () =>
            {
                _logger.Debug("Starting to send.");
                await stream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false);
                return original;
            });
        }

        public IObservable<Unit> ReceiveAndAcknowledge(Stream stream)
        {
            return Observable.FromAsync(async () =>
            {
                await ReceiveAndAcknowledgeAsync(stream);
            });
        }

        public async Task ReceiveAndAcknowledgeAsync(Stream stream)
        {
            var receivedBuffer = await stream.ReadBytesAsync(Constants.ReceivedBuffer.Length);
            if (receivedBuffer.SequenceEqual(Constants.ReceivedBuffer))
            {
                await stream.WriteAsync(Constants.AcknowledgedBuffer,
                                        0, Constants.AcknowledgedBuffer.Length);
            }
        }

        public IObservable<Tuple<byte[], OutgoingMessageBatch>> SerializeOutgoing(IObservable<OutgoingMessageBatch> outgoing)
        {
            _logger.Debug("Serializing outgoing");
            return from message in outgoing
                   select Tuple.Create(message.Messages.Serialize(), message);
        }
    }
}