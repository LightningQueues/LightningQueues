using System;
using System.IO;
using System.Threading.Tasks;
using System.Reactive;
using System.Reactive.Linq;

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
        public IObservable<IncomingMessage> SendStream(IObservable<OutgoingMessageBatch> observableMessages)
        {
            return from messageParts in SerializeOutgoing(observableMessages)
                   from _ in WriteLength(messageParts.Item2.Stream, messageParts.Item1.Length)
                   from messages in WriteMessages(messageParts.Item2.Stream, messageParts.Item1, messageParts.Item2.Messages)
                   from __ in ReceiveAndAcknowledge(messageParts.Item2.Stream)
                   from m in messages
                   select m;
        }

        public IObservable<Unit> WriteLength(Stream stream, int length)
        {
            var bufferLenInBytes = BitConverter.GetBytes(length);
            return Observable.FromAsync(() => stream.WriteAsync(bufferLenInBytes, 0, bufferLenInBytes.Length));
        }

        public IObservable<IncomingMessage[]> WriteMessages(Stream stream, byte[] messageBytes, IncomingMessage[] original)
        {
            return Observable.FromAsync(async () =>
            {
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
            byte[] receivedBuffer = new byte[Constants.ReceivedBuffer.Length];
            await stream.ReadAsyncToEnd(receivedBuffer);
            var result = System.Text.Encoding.Unicode.GetString(receivedBuffer);
            if (System.Text.Encoding.Unicode.GetString(receivedBuffer) == Constants.Received)
            {
                await stream.WriteAsync(Constants.AcknowledgedBuffer,
                                        0, Constants.AcknowledgedBuffer.Length);
            }
        }

        public IObservable<Tuple<byte[], OutgoingMessageBatch>> SerializeOutgoing(IObservable<OutgoingMessageBatch> outgoing)
        {
            return from message in outgoing
                   select Tuple.Create(message.Messages.Serialize(), message);
        }
    }
}