using System;
using System.IO;
using System.Reactive;
using System.Reactive.Linq;
using LightningQueues.Model;

namespace LightningQueues.Protocol.Reactive
{
    public interface ISendingProtocol
    {
        IObservable<Message> SendStream(IObservable<OutgoingMessageBatch> messages);
    }

    public class OutgoingMessageBatch
    {
        public Stream Stream { get; set; }
        public Message[] Messages { get; set; }
    }

    public class ReactiveSendingProtocol : ISendingProtocol
    {
        public IObservable<Message> SendStream(IObservable<OutgoingMessageBatch> observableMessages)
        {
            return from messageParts in SerializeOutgoing(observableMessages)
                   from _ in WriteLength(messageParts.Item2.Stream, messageParts.Item1.Length)
                   from messages in WriteMessages(messageParts.Item2.Stream, messageParts.Item1, messageParts.Item2.Messages)
                   from m in messages
                   select m;
        }

        public IObservable<Unit> WriteLength(Stream stream, int length)
        {
            var bufferLenInBytes = BitConverter.GetBytes(length);
            return Observable.FromAsync(() => stream.WriteAsync(bufferLenInBytes, 0, bufferLenInBytes.Length));
        }

        public IObservable<Message[]> WriteMessages(Stream stream, byte[] messageBytes, Message[] original)
        {
            return Observable.FromAsync(async () =>
            {
                await stream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false);
                return original;
            });
        }

        public IObservable<Tuple<byte[], OutgoingMessageBatch>> SerializeOutgoing(IObservable<OutgoingMessageBatch> outgoing)
        {
            return from message in outgoing
                   select Tuple.Create(message.Messages.Serialize(), message);
        }
    }
}