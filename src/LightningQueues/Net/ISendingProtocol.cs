using System;

namespace LightningQueues.Net
{
    public interface ISendingProtocol
    {
        IObservable<Message> SendStream(IObservable<OutgoingMessageBatch> messages);
    }
}