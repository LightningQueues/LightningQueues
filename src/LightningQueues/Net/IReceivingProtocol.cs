using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net;

public interface IReceivingProtocol
{
    Task<IList<Message>> ReceiveMessagesAsync(Stream stream, CancellationToken cancellationToken);
}