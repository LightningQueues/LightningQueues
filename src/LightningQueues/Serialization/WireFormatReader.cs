using System;
using System.Buffers.Binary;
using System.Text;

namespace LightningQueues.Serialization;

/// <summary>
/// Holds extracted outgoing message information without full deserialization.
/// Used for zero-copy sending where we need routing info but not full message content.
/// </summary>
public readonly struct RawOutgoingMessage
{
    /// <summary>16-byte MessageIdentifier used as LMDB key (for deletion after send).</summary>
    public ReadOnlyMemory<byte> MessageId { get; init; }

    /// <summary>Destination URI as UTF-8 bytes (for routing/grouping).</summary>
    public ReadOnlyMemory<byte> DestinationUriBytes { get; init; }

    /// <summary>Queue name as UTF-8 bytes (for grouping to isolate errors).</summary>
    public ReadOnlyMemory<byte> QueueNameBytes { get; init; }

    /// <summary>Complete wire-format bytes for this message (for sending).</summary>
    public ReadOnlyMemory<byte> FullMessage { get; init; }
}

/// <summary>
/// Efficient extraction of routing information from serialized messages WITHOUT full deserialization.
/// Parses wire format to extract only what's needed for sending: MessageId, destination, and queue.
/// </summary>
public static class WireFormatReader
{
    /// <summary>
    /// Extracts routing information from a single serialized message.
    /// Much faster than full deserialization - only calculates offsets and extracts key fields.
    /// </summary>
    /// <param name="messageBytes">Wire-format bytes for a single message (not a batch)</param>
    /// <returns>Extracted routing information with references to the original bytes</returns>
    public static RawOutgoingMessage ReadOutgoingMessage(ReadOnlyMemory<byte> messageBytes)
    {
        var span = messageBytes.Span;
        var pos = 0;

        // Skip SourceInstanceId (16 bytes)
        pos += 16;

        // MessageIdentifier (16 bytes) - this is our LMDB key
        var messageIdStart = pos;
        pos += 16;

        // Queue name (compressed string)
        var queueNameLength = Read7BitEncodedInt(span, ref pos);
        var queueNameStart = pos;
        pos += queueNameLength;

        // Skip SubQueue (compressed string)
        var subQueueLength = Read7BitEncodedInt(span, ref pos);
        pos += subQueueLength;

        // Skip SentAt (8 bytes)
        pos += 8;

        // Skip headers (4-byte count + key/value pairs)
        var headerCount = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(pos));
        pos += 4;
        for (var h = 0; h < headerCount; h++)
        {
            // Key (compressed string)
            var keyLength = Read7BitEncodedInt(span, ref pos);
            pos += keyLength;
            // Value (compressed string)
            var valueLength = Read7BitEncodedInt(span, ref pos);
            pos += valueLength;
        }

        // Skip Data (4-byte length + data bytes)
        var dataLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(pos));
        pos += 4 + dataLength;

        // DestinationUri (compressed string) - this is what we need for routing
        var destUriLength = Read7BitEncodedInt(span, ref pos);
        var destUriStart = pos;

        return new RawOutgoingMessage
        {
            MessageId = messageBytes.Slice(messageIdStart, 16),
            DestinationUriBytes = messageBytes.Slice(destUriStart, destUriLength),
            QueueNameBytes = messageBytes.Slice(queueNameStart, queueNameLength),
            FullMessage = messageBytes
        };
    }

    /// <summary>
    /// Extracts routing information from a single serialized message backed by an array.
    /// </summary>
    /// <param name="messageArray">Array containing the message bytes</param>
    /// <param name="offset">Offset into the array where the message starts</param>
    /// <param name="length">Length of the message bytes</param>
    /// <returns>Extracted routing information with references to the original array</returns>
    public static RawOutgoingMessage ReadOutgoingMessage(byte[] messageArray, int offset, int length)
    {
        return ReadOutgoingMessage(new ReadOnlyMemory<byte>(messageArray, offset, length));
    }

    /// <summary>
    /// Gets the destination URI as a string from RawOutgoingMessage.
    /// </summary>
    public static string GetDestinationUri(in RawOutgoingMessage message)
    {
        return Encoding.UTF8.GetString(message.DestinationUriBytes.Span);
    }

    /// <summary>
    /// Gets the queue name as a string from RawOutgoingMessage.
    /// </summary>
    public static string GetQueueName(in RawOutgoingMessage message)
    {
        return Encoding.UTF8.GetString(message.QueueNameBytes.Span);
    }

    /// <summary>
    /// Compares two destination URIs without allocating strings.
    /// </summary>
    public static bool DestinationsEqual(in RawOutgoingMessage a, in RawOutgoingMessage b)
    {
        return a.DestinationUriBytes.Span.SequenceEqual(b.DestinationUriBytes.Span);
    }

    /// <summary>
    /// Compares two queue names without allocating strings.
    /// </summary>
    public static bool QueueNamesEqual(in RawOutgoingMessage a, in RawOutgoingMessage b)
    {
        return a.QueueNameBytes.Span.SequenceEqual(b.QueueNameBytes.Span);
    }

    /// <summary>
    /// Checks if a destination URI matches a string without allocating.
    /// </summary>
    public static bool DestinationEquals(in RawOutgoingMessage message, string destination)
    {
        var destBytes = message.DestinationUriBytes.Span;
        var expectedByteCount = Encoding.UTF8.GetByteCount(destination);

        if (destBytes.Length != expectedByteCount)
            return false;

        Span<byte> expectedBytes = expectedByteCount <= 256
            ? stackalloc byte[expectedByteCount]
            : new byte[expectedByteCount];

        Encoding.UTF8.GetBytes(destination, expectedBytes);
        return destBytes.SequenceEqual(expectedBytes);
    }

    private static int Read7BitEncodedInt(ReadOnlySpan<byte> buffer, ref int offset)
    {
        var result = 0;
        var shift = 0;
        byte b;
        do
        {
            if (offset >= buffer.Length)
                throw new ArgumentException("Unexpected end of buffer while reading 7-bit encoded int");
            b = buffer[offset++];
            result |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 35);
        return result;
    }
}
