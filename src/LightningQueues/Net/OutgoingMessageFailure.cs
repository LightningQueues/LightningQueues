using System.Collections.Generic;

namespace LightningQueues.Net;

public class OutgoingMessageFailure
{
    public IList<Message> Messages { get; init; }
}