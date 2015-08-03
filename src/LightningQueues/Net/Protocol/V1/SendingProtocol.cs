using System;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;

namespace LightningQueues.Net.Protocol.V1
{
    public class SendingProtocol : ISendingProtocol
    {
        public IObservable<OutgoingMessage> Send(OutgoingMessageBatch batch)
        {
            return from outgoing in Observable.Return(batch)
                   let messageBytes = outgoing.Messages.Serialize()
                   let stream = outgoing.Stream
                   let lengthBytes = BitConverter.GetBytes(messageBytes.Length)
                   from _l in WriteLength()
                   from _m in Observable.FromAsync(() => stream.WriteAsync(messageBytes, 0, messageBytes.Length))
                   from _r in Observable.FromAsync(() => stream.ReadExpectedBuffer(Constants.ReceivedBuffer)).Where(x => x)
                   from _a in Observable.FromAsync(() => stream.WriteAsync(Constants.AcknowledgedBuffer, 0, Constants.AcknowledgedBuffer.Length))
                   from message in outgoing.Messages
                   select message;
        }

        public IObservable<Unit> WriteLength(Stream stream, int length)
        {
            var lengthBytes = BitConverter.GetBytes(length);
            return Observable.FromAsync(() => stream.WriteAsync(lengthBytes, 0, lengthBytes.Length));
        }
    }
}