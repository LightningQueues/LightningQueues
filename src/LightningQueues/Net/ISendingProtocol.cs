using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net;

public interface ISendingProtocol
{
    ValueTask SendAsync(Uri destination, Stream stream, List<Message> batch, CancellationToken token);
}