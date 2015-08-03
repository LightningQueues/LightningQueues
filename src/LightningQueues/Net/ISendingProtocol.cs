using System;
using System.IO;

namespace LightningQueues.Net
{
    public interface ISendingProtocol
    {
        IObservable<OutgoingMessage> Send(OutgoingMessageBatch batch);
    }
}