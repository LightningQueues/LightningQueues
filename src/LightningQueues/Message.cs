using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace LightningQueues;

/// <summary>
/// Represents a message that can be sent or received through a queue.
/// High-performance struct implementation that eliminates heap allocations.
/// </summary>
/// <remarks>
/// Messages are the core data structure in LightningQueues. Each message contains
/// the payload data, routing information, and metadata needed for delivery.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly struct Message
{
    /// <summary>
    /// Gets the unique identifier for this message.
    /// </summary>
    public readonly MessageId Id;

    /// <summary>
    /// Gets the name of the queue this message belongs to.
    /// </summary>
    public readonly ReadOnlyMemory<char> Queue;

    /// <summary>
    /// Gets the timestamp when this message was sent.
    /// </summary>
    public readonly DateTime SentAt;

    /// <summary>
    /// Gets the headers containing metadata about this message.
    /// </summary>
    /// <remarks>
    /// Headers can be used to store application-specific metadata, routing information,
    /// or other contextual data that should travel with the message.
    /// </remarks>
    public readonly FixedHeaders Headers;

    /// <summary>
    /// Gets the payload data of the message.
    /// </summary>
    /// <remarks>
    /// This contains the raw binary content of the message. The application is responsible
    /// for serializing and deserializing this data.
    /// </remarks>
    public readonly ReadOnlyMemory<byte> Data;

    /// <summary>
    /// Gets an optional sub-queue identifier for more granular routing.
    /// </summary>
    /// <remarks>
    /// Sub-queues can be used to categorize messages within a queue without
    /// requiring separate physical queues.
    /// </remarks>
    public readonly ReadOnlyMemory<char> SubQueue;

    /// <summary>
    /// Gets the URI destination for this message.
    /// </summary>
    /// <remarks>
    /// For outgoing messages, this specifies the endpoint where the message should be sent.
    /// The format is typically "lq.tcp://hostname:port".
    /// </remarks>
    public readonly ReadOnlyMemory<char> DestinationUri;

    /// <summary>
    /// Gets the time by which this message must be delivered.
    /// </summary>
    /// <remarks>
    /// If specified, the message will be considered expired if it cannot be
    /// delivered by this time. How expiration is handled depends on the queue configuration.
    /// </remarks>
    public readonly DateTime? DeliverBy;

    /// <summary>
    /// Gets the maximum number of delivery attempts for this message.
    /// </summary>
    /// <remarks>
    /// If specified, the queue will not attempt to deliver the message more than
    /// this number of times before considering it undeliverable.
    /// </remarks>
    public readonly int? MaxAttempts;

    /// <summary>
    /// Initializes a new instance of the <see cref="Message"/> struct.
    /// </summary>
    public Message(
        MessageId id,
        ReadOnlyMemory<byte> data,
        ReadOnlyMemory<char> queue,
        DateTime sentAt = default,
        ReadOnlyMemory<char> subQueue = default,
        ReadOnlyMemory<char> destinationUri = default,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        FixedHeaders headers = default,
        ReadOnlySequence<byte> bytes = default)
    {
        Id = id;
        Data = data;
        Queue = queue;
        SentAt = sentAt == default ? DateTime.UtcNow : sentAt;
        SubQueue = subQueue;
        DestinationUri = destinationUri;
        DeliverBy = deliverBy;
        MaxAttempts = maxAttempts;
        Headers = headers;
    }

    /// <summary>
    /// Creates a new message with string parameters for convenience.
    /// </summary>
    public static Message Create(
        byte[] data = null,
        string queue = null,
        string subQueue = null,
        string destinationUri = null,
        DateTime? deliverBy = null,
        int? maxAttempts = null,
        Dictionary<string, string> headers = null)
    {
        return new Message(
            id: MessageId.GenerateRandom(),
            data: data?.AsMemory() ?? default,
            queue: queue?.AsMemory() ?? default,
            subQueue: subQueue?.AsMemory() ?? default,
            destinationUri: destinationUri?.AsMemory() ?? default,
            deliverBy: deliverBy,
            maxAttempts: maxAttempts,
            headers: headers != null ? FixedHeaders.FromDictionary(headers) : default
        );
    }

    /// <summary>
    /// Gets the URI destination for this message
    /// </summary>
    public Uri Destination => DestinationUri.IsEmpty ? null : new Uri(DestinationUri.ToString());

    /// <summary>
    /// Gets the number of times this message has been sent
    /// </summary>
    public int SentAttempts => Headers.GetSentAttempts();

    /// <summary>
    /// Creates a new Message with updated sent attempts
    /// </summary>
    public Message WithSentAttempts(int attempts)
    {
        return new Message(
            Id, Data, Queue, SentAt, SubQueue, DestinationUri,
            DeliverBy, MaxAttempts, Headers.WithSentAttempts(attempts));
    }

    /// <summary>
    /// Creates a new Message with updated headers
    /// </summary>
    public Message WithHeaders(FixedHeaders headers)
    {
        return new Message(
            Id, Data, Queue, SentAt, SubQueue, DestinationUri,
            DeliverBy, MaxAttempts, headers);
    }

    /// <summary>
    /// Creates a new Message with updated bytes
    /// </summary>
    public Message WithBytes(ReadOnlySequence<byte> bytes)
    {
        return new Message(
            Id, Data, Queue, SentAt, SubQueue, DestinationUri,
            DeliverBy, MaxAttempts, Headers, bytes);
    }

    /// <summary>
    /// Gets the queue name as a string (allocates)
    /// </summary>
    public string QueueString => Queue.IsEmpty ? null : Queue.ToString();

    /// <summary>
    /// Gets the sub-queue name as a string (allocates)
    /// </summary>
    public string SubQueueString => SubQueue.IsEmpty ? null : SubQueue.ToString();

    /// <summary>
    /// Gets the data as a byte array (allocates)
    /// </summary>
    public byte[] DataArray => Data.IsEmpty ? null : Data.ToArray();

    /// <summary>
    /// Copies headers to a dictionary (allocates)
    /// </summary>
    public Dictionary<string, string> GetHeadersDictionary()
    {
        var dict = new Dictionary<string, string>();
        Headers.CopyTo(dict);
        return dict;
    }
}

/// <summary>
/// Fixed-size header storage to avoid Dictionary allocations for common scenarios
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct FixedHeaders
{
    private const int MaxHeaders = 4; // Most messages have 0-2 headers
    private const string SentAttemptsKey = "sent-attempts";
    
    // Cache common sent attempt values to avoid string allocations
    private static readonly ReadOnlyMemory<char>[] CachedSentAttempts = new ReadOnlyMemory<char>[11];
    private static readonly ReadOnlyMemory<char> SentAttemptsKeyMemory = SentAttemptsKey.AsMemory();
    
    static FixedHeaders()
    {
        for (int i = 0; i <= 10; i++)
        {
            CachedSentAttempts[i] = i.ToString().AsMemory();
        }
    }

    private readonly HeaderEntry _h0, _h1, _h2, _h3;
    private readonly byte _count;

    private readonly struct HeaderEntry(ReadOnlyMemory<char> key, ReadOnlyMemory<char> value)
    {
        public readonly ReadOnlyMemory<char> Key = key;
        public readonly ReadOnlyMemory<char> Value = value;

        public bool IsEmpty => Key.IsEmpty;
    }

    private FixedHeaders(ReadOnlySpan<(ReadOnlyMemory<char> key, ReadOnlyMemory<char> value)> headers)
    {
        var count = Math.Min(headers.Length, MaxHeaders);
        _count = (byte)count;

        _h0 = count > 0 ? new HeaderEntry(headers[0].key, headers[0].value) : default;
        _h1 = count > 1 ? new HeaderEntry(headers[1].key, headers[1].value) : default;
        _h2 = count > 2 ? new HeaderEntry(headers[2].key, headers[2].value) : default;
        _h3 = count > 3 ? new HeaderEntry(headers[3].key, headers[3].value) : default;
    }

    // Direct constructor for efficient header updates
    private FixedHeaders(HeaderEntry h0, HeaderEntry h1, HeaderEntry h2, HeaderEntry h3, byte count)
    {
        _h0 = h0;
        _h1 = h1;
        _h2 = h2;
        _h3 = h3;
        _count = count;
    }

    public static FixedHeaders FromDictionary(IDictionary<string, string> headers)
    {
        if (headers.Count == 0)
            return default;

        var headerArray = new (ReadOnlyMemory<char>, ReadOnlyMemory<char>)[Math.Min(headers.Count, MaxHeaders)];
        var headerSpan = headerArray.AsSpan();

        var index = 0;
        foreach (var kvp in headers)
        {
            if (index >= MaxHeaders) break;
            headerSpan[index] = (kvp.Key.AsMemory(), kvp.Value.AsMemory());
            index++;
        }

        return new FixedHeaders(headerSpan.Slice(0, index));
    }

    public void CopyTo(IDictionary<string, string> target)
    {
        target.Clear();
        
        if (_count > 0 && !_h0.IsEmpty) target[_h0.Key.ToString()] = _h0.Value.ToString();
        if (_count > 1 && !_h1.IsEmpty) target[_h1.Key.ToString()] = _h1.Value.ToString();
        if (_count > 2 && !_h2.IsEmpty) target[_h2.Key.ToString()] = _h2.Value.ToString();
        if (_count > 3 && !_h3.IsEmpty) target[_h3.Key.ToString()] = _h3.Value.ToString();
    }

    public int GetSentAttempts()
    {
        // Check each header for the sent-attempts key
        if (_count > 0 && _h0.Key.Span.SequenceEqual(SentAttemptsKey))
            return int.TryParse(_h0.Value.Span, out var val0) ? val0 : 0;
        if (_count > 1 && _h1.Key.Span.SequenceEqual(SentAttemptsKey))
            return int.TryParse(_h1.Value.Span, out var val1) ? val1 : 0;
        if (_count > 2 && _h2.Key.Span.SequenceEqual(SentAttemptsKey))
            return int.TryParse(_h2.Value.Span, out var val2) ? val2 : 0;
        if (_count > 3 && _h3.Key.Span.SequenceEqual(SentAttemptsKey))
            return int.TryParse(_h3.Value.Span, out var val3) ? val3 : 0;
        
        return 0;
    }

    public FixedHeaders WithSentAttempts(int attempts)
    {
        // Use cached values for common attempt counts to avoid allocations
        var attemptsStr = attempts <= 10 ? CachedSentAttempts[attempts] : attempts.ToString().AsMemory();

        // Check if we already have sent-attempts header - if so, can we reuse existing structure?
        var foundSentAttemptsIndex = -1;
        var sentAttemptsKeySpan = SentAttemptsKey.AsSpan();
        
        // Find existing sent-attempts header
        if (_count > 0 && !_h0.IsEmpty && _h0.Key.Span.SequenceEqual(sentAttemptsKeySpan))
            foundSentAttemptsIndex = 0;
        else if (_count > 1 && !_h1.IsEmpty && _h1.Key.Span.SequenceEqual(sentAttemptsKeySpan))
            foundSentAttemptsIndex = 1;
        else if (_count > 2 && !_h2.IsEmpty && _h2.Key.Span.SequenceEqual(sentAttemptsKeySpan))
            foundSentAttemptsIndex = 2;
        else if (_count > 3 && !_h3.IsEmpty && _h3.Key.Span.SequenceEqual(sentAttemptsKeySpan))
            foundSentAttemptsIndex = 3;

        // If found, create new FixedHeaders with updated value at same position
        if (foundSentAttemptsIndex >= 0)
        {
            return foundSentAttemptsIndex switch
            {
                0 => new FixedHeaders(new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), _h1, _h2, _h3, _count),
                1 => new FixedHeaders(_h0, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), _h2, _h3, _count),
                2 => new FixedHeaders(_h0, _h1, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), _h3, _count),
                3 => new FixedHeaders(_h0, _h1, _h2, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), _count),
                _ => this // Should never happen
            };
        }

        // Not found - add if there's space, otherwise fall back to array approach
        if (_count < MaxHeaders)
        {
            return _count switch
            {
                0 => new FixedHeaders(new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), default, default, default, 1),
                1 => new FixedHeaders(_h0, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), default, default, 2),
                2 => new FixedHeaders(_h0, _h1, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), default, 3),
                3 => new FixedHeaders(_h0, _h1, _h2, new HeaderEntry(SentAttemptsKeyMemory, attemptsStr), 4),
                _ => this // Should never happen
            };
        }

        // No space - return unchanged (sent-attempts will be ignored)
        return this;
    }

    public int Count => _count;

    /// <summary>
    /// Gets the header at the specified index for efficient iteration
    /// </summary>
    public (ReadOnlyMemory<char> key, ReadOnlyMemory<char> value) GetHeaderAt(int index)
    {
        return index switch
        {
            0 when _count > 0 && !_h0.IsEmpty => (_h0.Key, _h0.Value),
            1 when _count > 1 && !_h1.IsEmpty => (_h1.Key, _h1.Value),
            2 when _count > 2 && !_h2.IsEmpty => (_h2.Key, _h2.Value),
            3 when _count > 3 && !_h3.IsEmpty => (_h3.Key, _h3.Value),
            _ => (default, default)
        };
    }

    /// <summary>
    /// Creates FixedHeaders directly from individual entries without intermediate array allocation
    /// </summary>
    public static FixedHeaders CreateDirect(
        ReadOnlyMemory<char> key0 = default, ReadOnlyMemory<char> value0 = default,
        ReadOnlyMemory<char> key1 = default, ReadOnlyMemory<char> value1 = default,
        ReadOnlyMemory<char> key2 = default, ReadOnlyMemory<char> value2 = default,
        ReadOnlyMemory<char> key3 = default, ReadOnlyMemory<char> value3 = default)
    {
        var count = 0;
        if (!key0.IsEmpty) count++;
        if (!key1.IsEmpty) count++;
        if (!key2.IsEmpty) count++;
        if (!key3.IsEmpty) count++;

        return new FixedHeaders(
            key0.IsEmpty ? default : new HeaderEntry(key0, value0),
            key1.IsEmpty ? default : new HeaderEntry(key1, value1),
            key2.IsEmpty ? default : new HeaderEntry(key2, value2),
            key3.IsEmpty ? default : new HeaderEntry(key3, value3),
            (byte)count);
    }
}