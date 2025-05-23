using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using DotNext.Buffers;
using DotNext.IO;

namespace LightningQueues.Serialization;

public class MessageSerializer : IMessageSerializer
{
    private static readonly MemStringEqualityComparer Comp = new();
    private readonly ConcurrentDictionary<ReadOnlyMemory<char>, string> _commonStrings = new(Comp);
    private const int MaxCacheSize = 1000; // Prevent cache from growing too large
    
    // Thread-local pooling for PoolingBufferWriter instances to avoid contention
    [ThreadStatic]
    private static PoolingBufferWriter<byte> _tlsWriter;
    
    private static PoolingBufferWriter<byte> RentWriter()
    {
        var writer = _tlsWriter;
        if (writer != null)
        {
            _tlsWriter = null;
            writer.Clear();
            return writer;
        }
        return new PoolingBufferWriter<byte>(ArrayPool<byte>.Shared.ToAllocator());
    }

    private static void ReturnWriter(PoolingBufferWriter<byte> writer)
    {
        if (_tlsWriter == null)
        {
            writer.Clear();
            _tlsWriter = writer;
        }
        else
        {
            writer.Dispose();
        }
    }

    public ReadOnlyMemory<byte> ToMemory(List<Message> messages)
    {
        var writer = RentWriter();
        try
        {
            WriteMessages(writer, messages);
            return writer.WrittenMemory;
        }
        finally
        {
            ReturnWriter(writer);
        }
    }

    public IList<Message> ReadMessages(ReadOnlySequence<byte> buffer)
    {
        var reader = new SequenceReader(buffer);
        var numberOfMessages = reader.ReadLittleEndian<int>();
        var messages = new List<Message>(numberOfMessages);
            
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var msg = ReadMessage(ref reader);
            messages.Add(msg);
        }
        return messages;
    }
    
    private void WriteMessages(IBufferWriter<byte> writer, List<Message> messages)
    {
        writer.WriteLittleEndian(messages.Count);
        foreach (var message in messages)
        {
            WriteMessage(writer, message);
        }
    }

    private static void WriteMessage(IBufferWriter<byte> writer, Message message)
    {
        // Write message ID (32 bytes total)
        Span<byte> id = stackalloc byte[16];
        message.Id.SourceInstanceId.TryWriteBytes(id);
        writer.Write(id);
        message.Id.MessageIdentifier.TryWriteBytes(id);
        writer.Write(id);
        
        // Write strings using compressed format to match reader expectations  
        writer.Encode(message.Queue.Span, Encoding.UTF8, LengthFormat.Compressed);
        writer.Encode(message.SubQueue.Span, Encoding.UTF8, LengthFormat.Compressed);
        
        writer.WriteLittleEndian(message.SentAt.ToBinary());
        
        // Write headers directly from FixedHeaders without Dictionary allocation
        WriteFixedHeaders(writer, message.Headers);

        // Write data directly from ReadOnlyMemory
        writer.WriteLittleEndian(message.Data.Length);
        if (!message.Data.IsEmpty)
        {
            writer.Write(message.Data.Span);
        }

        // Write destination using compressed format
        writer.Encode(message.DestinationUri.Span, Encoding.UTF8, LengthFormat.Compressed);
        
        // Write optional fields
        writer.WriteLittleEndian(Convert.ToInt32(message.DeliverBy.HasValue));
        if (message.DeliverBy.HasValue)
        {
            writer.WriteLittleEndian(message.DeliverBy.Value.ToBinary());
        }

        writer.WriteLittleEndian(Convert.ToInt32(message.MaxAttempts.HasValue));
        if (message.MaxAttempts.HasValue)
        {
            writer.WriteLittleEndian(message.MaxAttempts.Value);
        } 
    }
    
    
    private static void WriteFixedHeaders(IBufferWriter<byte> writer, FixedHeaders headers)
    {
        writer.WriteLittleEndian(headers.Count);
        
        // Use FixedHeaders iterator to avoid Dictionary allocation
        for (int i = 0; i < headers.Count; i++)
        {
            var (key, value) = headers.GetHeaderAt(i);
            writer.Encode(key.Span, Encoding.UTF8, LengthFormat.Compressed);
            writer.Encode(value.Span, Encoding.UTF8, LengthFormat.Compressed);
        }
    }

    private static SequenceReader ReaderFor(IMemoryOwner<byte> owner, ReadOnlySpan<byte> buffer)
    {
        var memory = owner.Memory;
        
        buffer.CopyTo(memory.Span);
        
        var sequence = new ReadOnlySequence<byte>(memory);
        var reader = new SequenceReader(sequence);
        return reader;
    }

    public Message ToMessage(ReadOnlySpan<byte> buffer)
    {
        try
        {
            using var owner = MemoryPool<byte>.Shared.Rent(buffer.Length);
            var reader = ReaderFor(owner, buffer);
            return ReadMessage(ref reader);
        }
        catch (ArgumentOutOfRangeException)
        {
            // Convert deserialization issues into protocol violations to maintain compatibility
            throw new System.Net.ProtocolViolationException("Failed to deserialize message");
        }
    }

    private Message ReadMessage(ref SequenceReader reader)
    {
        var id = new MessageId
        {
            SourceInstanceId = ReadGuid(ref reader),
            MessageIdentifier = ReadGuid(ref reader)
        };
        
        // Read strings using the original efficient method
        var queue = ReadCommonStringAsMemory(ref reader);
        var subQueue = ReadCommonStringAsMemory(ref reader);
        var sentAt = DateTime.FromBinary(reader.ReadLittleEndian<long>());
        
        // Read headers efficiently
        var headers = ReadFixedHeadersEfficient(ref reader);

        // Read data efficiently - use ArrayPool for all data to reduce allocations
        var dataLength = reader.ReadLittleEndian<int>();
        ReadOnlyMemory<byte> data = default;
        if (dataLength > 0)
        {
            // Always use ArrayPool for consistency and to reduce GC pressure
            var rentedArray = ArrayPool<byte>.Shared.Rent(dataLength);
            try
            {
                var dataSpan = rentedArray.AsSpan(0, dataLength);
                reader.Read(dataSpan);
                // Copy to exact-size array since we can't return the rented array
                var dataArray = dataSpan.ToArray();
                data = dataArray.AsMemory();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedArray);
            }
        }
        
        var destinationUri = ReadCommonStringAsMemory(ref reader);
        
        var hasDeliverBy = Convert.ToBoolean(reader.ReadLittleEndian<int>());
        DateTime? deliverBy = null;
        if (hasDeliverBy)
        {
            deliverBy = DateTime.FromBinary(reader.ReadLittleEndian<long>()).ToLocalTime();
        }

        var hasMaxAttempts = Convert.ToBoolean(reader.ReadLittleEndian<int>());
        int? maxAttempts = null;
        if (hasMaxAttempts)
        {
            maxAttempts = reader.ReadLittleEndian<int>();
        }

        var msg = new Message(
            id: id,
            data: data,
            queue: queue,
            sentAt: sentAt,
            subQueue: subQueue,
            destinationUri: destinationUri,
            deliverBy: deliverBy,
            maxAttempts: maxAttempts,
            headers: headers
        );
        
        return msg;
    }
    
    
    private FixedHeaders ReadFixedHeadersEfficient(ref SequenceReader reader)
    {
        var headerCount = reader.ReadLittleEndian<int>();
        if (headerCount == 0)
            return default;
            
        var maxHeaders = Math.Min(headerCount, 4);
        
        // Read headers directly without intermediate array allocation
        ReadOnlyMemory<char> key0 = default, value0 = default;
        ReadOnlyMemory<char> key1 = default, value1 = default;
        ReadOnlyMemory<char> key2 = default, value2 = default;
        ReadOnlyMemory<char> key3 = default, value3 = default;
        
        if (maxHeaders > 0)
        {
            key0 = ReadCommonStringAsMemory(ref reader);   // Cache keys (likely to repeat)
            value0 = ReadStringAsMemory(ref reader);       // Don't cache values (arbitrary)
        }
        if (maxHeaders > 1)
        {
            key1 = ReadCommonStringAsMemory(ref reader);
            value1 = ReadStringAsMemory(ref reader);
        }
        if (maxHeaders > 2)
        {
            key2 = ReadCommonStringAsMemory(ref reader);
            value2 = ReadStringAsMemory(ref reader);
        }
        if (maxHeaders > 3)
        {
            key3 = ReadCommonStringAsMemory(ref reader);
            value3 = ReadStringAsMemory(ref reader);
        }
        
        // Skip remaining headers if more than 4
        for (var i = 4; i < headerCount; i++)
        {
            ReadCommonStringAsMemory(ref reader); // key
            ReadStringAsMemory(ref reader);       // value - don't cache
        }
        
        return FixedHeaders.CreateDirect(key0, value0, key1, value1, key2, value2, key3, value3);
    }

    private static Guid ReadGuid(ref SequenceReader reader)
    {
        Span<byte> guidBuffer = stackalloc byte[16];
        reader.Read(guidBuffer);
        return new Guid(guidBuffer);
    }

    public ReadOnlySpan<byte> AsSpan(Message message)
    {
        var writer = RentWriter();
        try
        {
            WriteMessage(writer, message);
            return writer.WrittenMemory.Span;
        }
        finally
        {
            ReturnWriter(writer);
        }
    }

    private ReadOnlyMemory<char> ReadCommonStringAsMemory(ref SequenceReader reader)
    {
        var stringBlock = reader.Decode(Encoding.UTF8, LengthFormat.Compressed);
        
        // For common strings, return the cached version's memory
        if (_commonStrings.TryGetValue(stringBlock.Memory, out var commonString))
        {
            return commonString.AsMemory();
        }
        
        // Cache new string and return its memory (only if cache isn't too large)
        var newString = stringBlock.ToString();
        if (_commonStrings.Count < MaxCacheSize)
        {
            _commonStrings[stringBlock.Memory] = newString;
        }
        return newString.AsMemory();
    }
    
    private ReadOnlyMemory<char> ReadStringAsMemory(ref SequenceReader reader)
    {
        // Read string without caching - for arbitrary values like header values
        var stringBlock = reader.Decode(Encoding.UTF8, LengthFormat.Compressed);
        return stringBlock.ToString().AsMemory();
    }
}