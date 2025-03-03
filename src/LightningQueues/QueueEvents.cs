using Microsoft.Extensions.Logging;

namespace LightningQueues;

internal static class QueueEvents
{
    internal static EventId Sender = new(1000, "Sender");
    internal static EventId Receiver = new(1001, "Receiver");
    internal static EventId Protocol = new(1002, "Protocol");
    internal static EventId Queue = new(1003, "Queue");
}