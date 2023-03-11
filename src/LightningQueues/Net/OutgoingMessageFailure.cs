using System.Collections.Generic;

namespace LightningQueues.Net;

public class OutgoingMessageFailure
{
    public IList<OutgoingMessage> Messages { get; init; }
}