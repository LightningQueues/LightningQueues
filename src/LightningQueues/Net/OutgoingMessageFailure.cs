using System.Collections.Generic;

namespace LightningQueues.Net;

public class OutgoingMessageFailure
{
    public bool ShouldRetry { get; init; }
    public IList<Message> Messages { get; init; }
}