using System;
using System.Buffers;
using System.Collections.Generic;

namespace LightningQueues;

/// <summary>
/// Represents a message that can be sent or received through a queue.
/// </summary>
/// <remarks>
/// Messages are the core data structure in LightningQueues. Each message contains
/// the payload data, routing information, and metadata needed for delivery.
/// </remarks>
public class Message
{
    private const string SentAttemptsHeaderKey = "sent-attempts";

    /// <summary>
    /// Initializes a new instance of the <see cref="Message"/> class.
    /// </summary>
    /// <remarks>
    /// Creates a new message with initialized headers dictionary and the current 
    /// UTC timestamp as the sent time.
    /// </remarks>
    public Message()
    {
        Headers = new Dictionary<string, string>();
        SentAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Gets the unique identifier for this message.
    /// </summary>
    public MessageId Id { get; init; }

    /// <summary>
    /// Gets or sets the name of the queue this message belongs to.
    /// </summary>
    public string Queue { get; set; }

    /// <summary>
    /// Gets or sets the timestamp when this message was sent.
    /// </summary>
    public DateTime SentAt { get; set; }

    /// <summary>
    /// Gets a dictionary of key-value pairs containing metadata about this message.
    /// </summary>
    /// <remarks>
    /// Headers can be used to store application-specific metadata, routing information,
    /// or other contextual data that should travel with the message.
    /// </remarks>
    public IDictionary<string, string> Headers { get; }

    /// <summary>
    /// Gets or sets the payload data of the message.
    /// </summary>
    /// <remarks>
    /// This contains the raw binary content of the message. The application is responsible
    /// for serializing and deserializing this data.
    /// </remarks>
    public byte[] Data { get; set; }

    /// <summary>
    /// Gets or sets an optional sub-queue identifier for more granular routing.
    /// </summary>
    /// <remarks>
    /// Sub-queues can be used to categorize messages within a queue without
    /// requiring separate physical queues.
    /// </remarks>
    public string SubQueue { get; set; }

    /// <summary>
    /// Gets or sets the URI destination for this message.
    /// </summary>
    /// <remarks>
    /// For outgoing messages, this specifies the endpoint where the message should be sent.
    /// The format is typically "lq.tcp://hostname:port".
    /// </remarks>
    public Uri Destination { get; set; }

    /// <summary>
    /// Gets or sets the time by which this message must be delivered.
    /// </summary>
    /// <remarks>
    /// If specified, the message will be considered expired if it cannot be
    /// delivered by this time. How expiration is handled depends on the queue configuration.
    /// </remarks>
    public DateTime? DeliverBy { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of delivery attempts for this message.
    /// </summary>
    /// <remarks>
    /// If specified, the queue will not attempt to deliver the message more than
    /// this number of times before considering it undeliverable.
    /// </remarks>
    public int? MaxAttempts { get; set; }

    /// <summary>
    /// Gets or sets the number of times this message has been sent.
    /// </summary>
    /// <remarks>
    /// This counter is used to track delivery attempts and is stored in the message
    /// headers. It can be used in conjunction with MaxAttempts to limit retries.
    /// </remarks>
    public int SentAttempts
    {
        get => Headers.TryGetValue(SentAttemptsHeaderKey, out var value) 
            ? int.Parse(value) : 0;
        set => Headers[SentAttemptsHeaderKey] = value.ToString();
    }
        
    internal ReadOnlySequence<byte> Bytes { get; set; }
}