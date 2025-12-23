using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace LightningQueues.Serialization;

public class MessageSerializer : IMessageSerializer
{
    private static readonly MemStringEqualityComparer Comp = new();
    private readonly ConcurrentDictionary<ReadOnlyMemory<char>, string> _commonStrings = new(Comp);
    private const int MaxCacheSize = 1000; // Prevent cache from growing too large

    // Thread-local pooling for PooledBufferWriter instances to avoid contention
    [ThreadStatic]
    private static PooledBufferWriter? _tlsWriter;

    private static PooledBufferWriter RentWriter()
    {
        var writer = _tlsWriter;
        if (writer != null)
        {
            _tlsWriter = null;
            writer.Clear();
            return writer;
        }
        return new PooledBufferWriter(256);
    }

    private static void ReturnWriter(PooledBufferWriter writer)
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
        var reader = new SequenceReader<byte>(buffer);
        if (!reader.TryReadLittleEndian(out int numberOfMessages))
            throw new EndOfStreamException("Failed to read message count");

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
        WriteLittleEndian(writer, messages.Count);
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
        WriteCompressedString(writer, message.Queue.Span);
        WriteCompressedString(writer, message.SubQueue.Span);

        WriteLittleEndian(writer, message.SentAt.ToBinary());

        // Write headers directly from FixedHeaders without Dictionary allocation
        WriteFixedHeaders(writer, message.Headers);

        // Write data directly from ReadOnlyMemory
        WriteLittleEndian(writer, message.Data.Length);
        if (!message.Data.IsEmpty)
        {
            writer.Write(message.Data.Span);
        }

        // Write destination using compressed format
        WriteCompressedString(writer, message.DestinationUri.Span);

        // Write optional fields
        WriteLittleEndian(writer, Convert.ToInt32(message.DeliverBy.HasValue));
        if (message.DeliverBy.HasValue)
        {
            WriteLittleEndian(writer, message.DeliverBy.Value.ToBinary());
        }

        WriteLittleEndian(writer, Convert.ToInt32(message.MaxAttempts.HasValue));
        if (message.MaxAttempts.HasValue)
        {
            WriteLittleEndian(writer, message.MaxAttempts.Value);
        }
    }


    private static void WriteFixedHeaders(IBufferWriter<byte> writer, FixedHeaders headers)
    {
        WriteLittleEndian(writer, headers.Count);

        // Use FixedHeaders iterator to avoid Dictionary allocation
        for (int i = 0; i < headers.Count; i++)
        {
            var (key, value) = headers.GetHeaderAt(i);
            WriteCompressedString(writer, key.Span);
            WriteCompressedString(writer, value.Span);
        }
    }

    public Message ToMessage(ReadOnlySpan<byte> buffer)
    {
        try
        {
            using var owner = MemoryPool<byte>.Shared.Rent(buffer.Length);
            var memory = owner.Memory;
            buffer.CopyTo(memory.Span);

            var sequence = new ReadOnlySequence<byte>(memory.Slice(0, buffer.Length));
            var reader = new SequenceReader<byte>(sequence);
            return ReadMessage(ref reader);
        }
        catch (ArgumentOutOfRangeException)
        {
            // Convert deserialization issues into protocol violations to maintain compatibility
            throw new System.Net.ProtocolViolationException("Failed to deserialize message");
        }
    }

    private Message ReadMessage(ref SequenceReader<byte> reader)
    {
        var id = new MessageId
        {
            SourceInstanceId = ReadGuid(ref reader),
            MessageIdentifier = ReadGuid(ref reader)
        };

        // Read strings using the efficient method
        var queue = ReadCommonStringAsMemory(ref reader);
        var subQueue = ReadCommonStringAsMemory(ref reader);

        if (!reader.TryReadLittleEndian(out long sentAtBinary))
            throw new EndOfStreamException("Failed to read SentAt");
        var sentAt = DateTime.FromBinary(sentAtBinary);

        // Read headers efficiently
        var headers = ReadFixedHeadersEfficient(ref reader);

        // Read data efficiently - use ArrayPool for all data to reduce allocations
        if (!reader.TryReadLittleEndian(out int dataLength))
            throw new EndOfStreamException("Failed to read data length");

        ReadOnlyMemory<byte> data = default;
        if (dataLength > 0)
        {
            // Always use ArrayPool for consistency and to reduce GC pressure
            var rentedArray = ArrayPool<byte>.Shared.Rent(dataLength);
            try
            {
                var dataSpan = rentedArray.AsSpan(0, dataLength);
                if (!reader.TryCopyTo(dataSpan))
                    throw new EndOfStreamException("Failed to read message data");
                reader.Advance(dataLength);
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

        if (!reader.TryReadLittleEndian(out int hasDeliverByInt))
            throw new EndOfStreamException("Failed to read DeliverBy flag");
        var hasDeliverBy = Convert.ToBoolean(hasDeliverByInt);
        DateTime? deliverBy = null;
        if (hasDeliverBy)
        {
            if (!reader.TryReadLittleEndian(out long deliverByBinary))
                throw new EndOfStreamException("Failed to read DeliverBy");
            deliverBy = DateTime.FromBinary(deliverByBinary).ToLocalTime();
        }

        if (!reader.TryReadLittleEndian(out int hasMaxAttemptsInt))
            throw new EndOfStreamException("Failed to read MaxAttempts flag");
        var hasMaxAttempts = Convert.ToBoolean(hasMaxAttemptsInt);
        int? maxAttempts = null;
        if (hasMaxAttempts)
        {
            if (!reader.TryReadLittleEndian(out int maxAttemptsValue))
                throw new EndOfStreamException("Failed to read MaxAttempts");
            maxAttempts = maxAttemptsValue;
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


    private FixedHeaders ReadFixedHeadersEfficient(ref SequenceReader<byte> reader)
    {
        if (!reader.TryReadLittleEndian(out int headerCount))
            throw new EndOfStreamException("Failed to read header count");

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

    private static Guid ReadGuid(ref SequenceReader<byte> reader)
    {
        Span<byte> guidBuffer = stackalloc byte[16];
        if (!reader.TryCopyTo(guidBuffer))
            throw new EndOfStreamException("Failed to read GUID");
        reader.Advance(16);
        return new Guid(guidBuffer);
    }

    public ReadOnlySpan<byte> AsSpan(Message message)
    {
        var writer = RentWriter();
        try
        {
            WriteMessage(writer, message);
            return writer.WrittenSpan;
        }
        finally
        {
            ReturnWriter(writer);
        }
    }

    private ReadOnlyMemory<char> ReadCommonStringAsMemory(ref SequenceReader<byte> reader)
    {
        var str = ReadCompressedString(ref reader);

        // For common strings, return the cached version's memory
        var memory = str.AsMemory();
        if (_commonStrings.TryGetValue(memory, out var commonString))
        {
            return commonString.AsMemory();
        }

        // Cache new string and return its memory (only if cache isn't too large)
        if (_commonStrings.Count < MaxCacheSize)
        {
            _commonStrings[memory] = str;
        }
        return memory;
    }

    private static ReadOnlyMemory<char> ReadStringAsMemory(ref SequenceReader<byte> reader)
    {
        // Read string without caching - for arbitrary values like header values
        var str = ReadCompressedString(ref reader);
        return str.AsMemory();
    }

    // ═══════════════════════════════════════════════════════════════════
    // 7-bit encoding helpers (same format as BinaryReader/BinaryWriter)
    // This is compatible with DotNext's LengthFormat.Compressed
    // ═══════════════════════════════════════════════════════════════════

    private static void WriteLittleEndian(IBufferWriter<byte> writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteLittleEndian(IBufferWriter<byte> writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteCompressedString(IBufferWriter<byte> writer, ReadOnlySpan<char> value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        Write7BitEncodedInt(writer, byteCount);

        if (byteCount > 0)
        {
            var span = writer.GetSpan(byteCount);
            Encoding.UTF8.GetBytes(value, span);
            writer.Advance(byteCount);
        }
    }

    private static void Write7BitEncodedInt(IBufferWriter<byte> writer, int value)
    {
        Span<byte> buffer = stackalloc byte[5]; // Max 5 bytes for 32-bit int
        int index = 0;
        uint v = (uint)value;
        while (v > 0x7F)
        {
            buffer[index++] = (byte)(v | 0x80);
            v >>= 7;
        }
        buffer[index++] = (byte)v;
        writer.Write(buffer[..index]);
    }

    private static string ReadCompressedString(ref SequenceReader<byte> reader)
    {
        int length = Read7BitEncodedInt(ref reader);
        if (length == 0)
            return string.Empty;

        // Use stackalloc for small strings, array for larger ones
        if (length <= 256)
        {
            Span<byte> utf8Bytes = stackalloc byte[length];
            if (!reader.TryCopyTo(utf8Bytes))
                throw new EndOfStreamException("Failed to read string bytes");
            reader.Advance(length);
            return Encoding.UTF8.GetString(utf8Bytes);
        }
        else
        {
            var rentedArray = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                var utf8Bytes = rentedArray.AsSpan(0, length);
                if (!reader.TryCopyTo(utf8Bytes))
                    throw new EndOfStreamException("Failed to read string bytes");
                reader.Advance(length);
                return Encoding.UTF8.GetString(utf8Bytes);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedArray);
            }
        }
    }

    private static int Read7BitEncodedInt(ref SequenceReader<byte> reader)
    {
        int result = 0;
        int shift = 0;
        byte b;
        do
        {
            if (!reader.TryRead(out b))
                throw new EndOfStreamException("Failed to read 7-bit encoded int");
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 35);
        return result;
    }
}
