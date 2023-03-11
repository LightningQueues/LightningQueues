using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace LightningQueues.Net;

public interface IReceivingProtocol
{
    IAsyncEnumerable<Message> ReceiveMessagesAsync(Stream stream, CancellationToken cancellationToken);
}