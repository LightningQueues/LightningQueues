using System;

namespace LightningQueues.Net
{
    public interface ISendingProtocol
    {
        IObservable<IncomingMessage> SendStream(IObservable<OutgoingMessageBatch> messages);
    }
}