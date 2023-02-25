using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;

namespace LightningQueues.Net;

public interface IReceivingProtocol
{
    IAsyncEnumerable<Message> ReceiveMessagesAsync(EndPoint endpoint, Stream stream,
        CancellationToken cancellationToken);
}