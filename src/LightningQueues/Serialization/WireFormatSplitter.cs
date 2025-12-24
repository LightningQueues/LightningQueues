using System;
using System.Buffers.Binary;
using System.Text;

namespace LightningQueues.Serialization;

/// <summary>
/// Holds extracted message information without full deserialization.
/// Used for zero-copy storage where we only need the key (MessageId) and queue name.
/// </summary>
public readonly struct RawMessageInfo
{
    /// <summary>16-byte MessageIdentifier used as LMDB key.</summary>
    public ReadOnlyMemory<byte> MessageId { get; init; }

    /// <summary>Queue name as UTF-8 bytes (for routing).</summary>
    public ReadOnlyMemory<byte> QueueNameBytes { get; init; }

    /// <summary>Complete wire-format bytes for this message (for storage).</summary>
    public ReadOnlyMemory<byte> FullMessage { get; init; }
}

/// <summary>
/// Fast message boundary detection and key extraction WITHOUT full deserialization.
/// Parses wire format to extract only what's needed for storage: MessageId and queue name.
/// </summary>
public static class WireFormatSplitter
{
    /// <summary>
    /// Splits a serialized batch into individual message info structs.
    /// Only extracts MessageId and queue name - does not deserialize full message content.
    /// </summary>
    /// <param name="batchBytes">Wire-format batch (4-byte count + messages)</param>
    /// <param name="output">Pre-allocated span to receive message info</param>
    /// <returns>Number of messages extracted</returns>
    public static int SplitBatch(ReadOnlySpan<byte> batchBytes, Span<RawMessageInfo> output)
    {
        if (batchBytes.Length < 4)
            return 0;

        var messageCount = BinaryPrimitives.ReadInt32LittleEndian(batchBytes);
        if (messageCount > output.Length)
            throw new ArgumentException($"Output span too small: need {messageCount}, have {output.Length}");

        var offset = 4; // Skip message count

        for (var i = 0; i < messageCount && offset < batchBytes.Length; i++)
        {
            var messageStart = offset;

            // Skip SourceInstanceId (16 bytes)
            offset += 16;

            // MessageIdentifier (16 bytes) - this is our LMDB key
            var messageIdStart = offset;
            offset += 16;

            // Queue name (compressed string)
            var queueNameStart = offset;
            var queueNameLength = Read7BitEncodedInt(batchBytes, ref offset);
            offset += queueNameLength;
            var queueNameEnd = offset;

            // Skip SubQueue (compressed string)
            var subQueueLength = Read7BitEncodedInt(batchBytes, ref offset);
            offset += subQueueLength;

            // Skip SentAt (8 bytes)
            offset += 8;

            // Skip headers
            var headerCount = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(offset));
            offset += 4;
            for (var h = 0; h < headerCount; h++)
            {
                // Key
                var keyLength = Read7BitEncodedInt(batchBytes, ref offset);
                offset += keyLength;
                // Value
                var valueLength = Read7BitEncodedInt(batchBytes, ref offset);
                offset += valueLength;
            }

            // Data (4-byte length + data)
            var dataLength = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(offset));
            offset += 4 + dataLength;

            // Skip DestinationUri (compressed string)
            var destLength = Read7BitEncodedInt(batchBytes, ref offset);
            offset += destLength;

            // Skip DeliverBy (4-byte flag + optional 8 bytes)
            var hasDeliverBy = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(offset));
            offset += 4;
            if (hasDeliverBy != 0)
                offset += 8;

            // Skip MaxAttempts (4-byte flag + optional 4 bytes)
            var hasMaxAttempts = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(offset));
            offset += 4;
            if (hasMaxAttempts != 0)
                offset += 4;

            var messageEnd = offset;

            // Note: We need to convert to Memory for storage since Span can't be stored in struct
            // The caller must ensure batchBytes is backed by an array or pinned memory
            output[i] = new RawMessageInfo
            {
                MessageId = default,     // Will be set by caller from array
                QueueNameBytes = default, // Will be set by caller from array
                FullMessage = default     // Will be set by caller from array
            };
        }

        return messageCount;
    }

    /// <summary>
    /// Splits a batch backed by an array, returning RawMessageInfo with proper Memory references.
    /// Throws FormatException if the data is malformed.
    /// </summary>
    public static int SplitBatch(byte[] batchArray, int offset, int length, RawMessageInfo[] output)
    {
        var batchBytes = batchArray.AsSpan(offset, length);

        if (length < 4)
            throw new FormatException("Buffer too small to contain message count");

        var messageCount = BinaryPrimitives.ReadInt32LittleEndian(batchBytes);
        if (messageCount < 0 || messageCount > 10000)
            throw new FormatException($"Invalid message count: {messageCount}");
        if (messageCount > output.Length)
            throw new ArgumentException($"Output array too small: need {messageCount}, have {output.Length}");

        var pos = 4; // Skip message count

        for (var i = 0; i < messageCount; i++)
        {
            var messageStart = pos;

            // Skip SourceInstanceId (16 bytes)
            EnsureBytes(pos, 16, length, "SourceInstanceId");
            pos += 16;

            // MessageIdentifier (16 bytes) - this is our LMDB key
            EnsureBytes(pos, 16, length, "MessageIdentifier");
            var messageIdStart = pos;
            pos += 16;

            // Queue name (compressed string)
            var queueNameLength = Read7BitEncodedIntChecked(batchBytes, ref pos, length, "QueueName length");
            EnsureBytes(pos, queueNameLength, length, "QueueName");
            var queueNameBytesStart = pos;
            pos += queueNameLength;

            // Skip SubQueue (compressed string)
            var subQueueLength = Read7BitEncodedIntChecked(batchBytes, ref pos, length, "SubQueue length");
            EnsureBytes(pos, subQueueLength, length, "SubQueue");
            pos += subQueueLength;

            // Skip SentAt (8 bytes)
            EnsureBytes(pos, 8, length, "SentAt");
            pos += 8;

            // Skip headers
            EnsureBytes(pos, 4, length, "Header count");
            var headerCount = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(pos));
            if (headerCount < 0 || headerCount > 1000)
                throw new FormatException($"Invalid header count: {headerCount}");
            pos += 4;
            for (var h = 0; h < headerCount; h++)
            {
                // Key
                var keyLength = Read7BitEncodedIntChecked(batchBytes, ref pos, length, $"Header[{h}] key length");
                EnsureBytes(pos, keyLength, length, $"Header[{h}] key");
                pos += keyLength;
                // Value
                var valueLength = Read7BitEncodedIntChecked(batchBytes, ref pos, length, $"Header[{h}] value length");
                EnsureBytes(pos, valueLength, length, $"Header[{h}] value");
                pos += valueLength;
            }

            // Data (4-byte length + data)
            EnsureBytes(pos, 4, length, "Data length");
            var dataLength = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(pos));
            if (dataLength < 0)
                throw new FormatException($"Invalid data length: {dataLength}");
            pos += 4;
            EnsureBytes(pos, dataLength, length, "Data");
            pos += dataLength;

            // Skip DestinationUri (compressed string)
            var destLength = Read7BitEncodedIntChecked(batchBytes, ref pos, length, "DestinationUri length");
            EnsureBytes(pos, destLength, length, "DestinationUri");
            pos += destLength;

            // Skip DeliverBy (4-byte flag + optional 8 bytes)
            EnsureBytes(pos, 4, length, "DeliverBy flag");
            var hasDeliverBy = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(pos));
            pos += 4;
            if (hasDeliverBy != 0)
            {
                EnsureBytes(pos, 8, length, "DeliverBy value");
                pos += 8;
            }

            // Skip MaxAttempts (4-byte flag + optional 4 bytes)
            EnsureBytes(pos, 4, length, "MaxAttempts flag");
            var hasMaxAttempts = BinaryPrimitives.ReadInt32LittleEndian(batchBytes.Slice(pos));
            pos += 4;
            if (hasMaxAttempts != 0)
            {
                EnsureBytes(pos, 4, length, "MaxAttempts value");
                pos += 4;
            }

            var messageEnd = pos;

            output[i] = new RawMessageInfo
            {
                MessageId = new ReadOnlyMemory<byte>(batchArray, offset + messageIdStart, 16),
                QueueNameBytes = new ReadOnlyMemory<byte>(batchArray, offset + queueNameBytesStart, queueNameLength),
                FullMessage = new ReadOnlyMemory<byte>(batchArray, offset + messageStart, messageEnd - messageStart)
            };
        }

        return messageCount;
    }

    private static void EnsureBytes(int pos, int needed, int length, string field)
    {
        if (pos + needed > length)
            throw new FormatException($"Unexpected end of data reading {field}: need {needed} bytes at position {pos}, but only {length - pos} available");
    }

    private static int Read7BitEncodedIntChecked(ReadOnlySpan<byte> buffer, ref int offset, int length, string field)
    {
        var result = 0;
        var shift = 0;
        byte b;
        do
        {
            if (offset >= length)
                throw new FormatException($"Unexpected end of data reading {field}");
            b = buffer[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 35);

        if (result < 0)
            throw new FormatException($"Invalid {field}: {result}");
        return result;
    }

    /// <summary>
    /// Gets the queue name as a string from RawMessageInfo.
    /// </summary>
    public static string GetQueueName(in RawMessageInfo info)
    {
        return Encoding.UTF8.GetString(info.QueueNameBytes.Span);
    }

    private static int Read7BitEncodedInt(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var result = 0;
        var shift = 0;
        byte b;
        do
        {
            b = buffer[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 35);
        return result;
    }
}
