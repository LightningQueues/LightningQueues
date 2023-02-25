using System;

namespace LightningQueues;

public class OutgoingMessage : Message
{
    private const string SentAttemptsHeaderKey = "sent-attempts";

    public Uri Destination { get; set; }
    public DateTime? DeliverBy { get; set; }
    public int? MaxAttempts { get; set; }

    public int SentAttempts
    {
        get => Headers.TryGetValue(SentAttemptsHeaderKey, out var value) 
            ? int.Parse(value) : 0;
        set => Headers[SentAttemptsHeaderKey] = value.ToString();
    }
}