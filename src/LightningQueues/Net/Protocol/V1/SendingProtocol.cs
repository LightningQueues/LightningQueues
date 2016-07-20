using System;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1
{
    public class SendingProtocol : ISendingProtocol
    {
        private readonly IMessageStore _store;

        public SendingProtocol(IMessageStore store)
        {
            _store = store;
        }

        public IObservable<OutgoingMessage> Send(OutgoingMessageBatch batch)
        {
            return from outgoing in Observable.Return(batch)
                   let messageBytes = outgoing.Messages.Serialize()
                   let stream = outgoing.Stream
                   from _l in WriteLength(stream, messageBytes.Length)
                   from _m in WriteMessages(stream, messageBytes)
                   from _r in ReadReceived(stream)
                   from _a in WriteAcknowledgement(stream).Do(_ => _store.SuccessfullySent(outgoing.Messages.ToArray()))
                   from message in outgoing.Messages
                   select message;
        }

        public IObservable<Unit> WriteLength(Stream stream, int length)
        {
            var lengthBytes = BitConverter.GetBytes(length);
            return Observable.FromAsync(async () => await stream.WriteAsync(lengthBytes, 0, lengthBytes.Length).ConfigureAwait(false));
        }

        public IObservable<Unit> WriteMessages(Stream stream, byte[] messageBytes)
        {
            return Observable.FromAsync(async () => await stream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false));
        }

        public IObservable<Unit> ReadReceived(Stream stream)
        {
            return Observable.FromAsync(async () =>
            {
                var bytes = await stream.ReadBytesAsync(Constants.ReceivedBuffer.Length).ConfigureAwait(false);
                if (bytes.SequenceEqual(Constants.ReceivedBuffer))
                {
                    return true;
                }
                if (bytes.SequenceEqual(Constants.SerializationFailureBuffer))
                {
                    throw new IOException("Failed to send messages, received serialization failed message.");
                }
                return false;
            }).Where(x => x).Select(x => Unit.Default);
        }

        public IObservable<Unit> WriteAcknowledgement(Stream stream)
        {
            return Observable.FromAsync(async () => await stream.WriteAsync(Constants.AcknowledgedBuffer, 0, Constants.AcknowledgedBuffer.Length).ConfigureAwait(false));
        }
    }
}