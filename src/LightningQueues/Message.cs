using System;
using System.Buffers;
using System.Collections.Generic;

namespace LightningQueues;

public class Message
{
    public Message()
    {
        Headers = new Dictionary<string, string>();
        SentAt = DateTime.UtcNow;
    }

    public MessageId Id { get; init; }
    public string Queue { get; set; }
    public DateTime SentAt { get; set; }
    public IDictionary<string, string> Headers { get; }
    public byte[] Data { get; set; }
    public string SubQueue { get; set; }

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
        
    internal ReadOnlySequence<byte> Bytes { get; set; }
}