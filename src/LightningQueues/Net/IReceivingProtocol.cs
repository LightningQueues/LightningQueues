using System;
using System.IO;

namespace LightningQueues.Net
{
    public interface IReceivingProtocol
    {
        IObservable<IncomingMessage> ReceiveStream(IObservable<Stream> streams);
    }
}
