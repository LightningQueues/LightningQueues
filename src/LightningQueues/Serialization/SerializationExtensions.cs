using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using DotNext.Buffers;
using DotNext.IO;

namespace LightningQueues.Serialization;

public static class SerializationExtensions
{
    public static void WriteOutgoingMessages(this IBufferWriter<byte> writer, List<OutgoingMessage> messages)
    {
        writer.WriteInt32(messages.Count, true);
        foreach (var message in messages)
        {
            if (message.Bytes.Length > 0)
            {
                writer.Write(message.Bytes);
                continue;
            }
            WriteMessage(writer, message);
            writer.WriteString(message.Destination.OriginalString, Encoding.UTF8, LengthFormat.Compressed);
            writer.Write(message.DeliverBy.HasValue);
            if (message.DeliverBy.HasValue)
            {
                writer.Write(message.DeliverBy.Value.ToBinary());
            }

            writer.Write(message.MaxAttempts.HasValue);
            if (message.MaxAttempts.HasValue)
            {
                writer.WriteInt32(message.MaxAttempts.Value, true);
            }
        }
    }

    private static void WriteMessage(IBufferWriter<byte> writer, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.SourceInstanceId.TryWriteBytes(id);
        writer.Write(id);
        message.Id.MessageIdentifier.TryWriteBytes(id);
        writer.Write(id);
        writer.WriteString(message.Queue.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
        writer.WriteString((message.SubQueue ?? string.Empty).AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
        writer.Write(message.SentAt.ToBinary());

        writer.Write(message.Headers.Count);
        foreach (var pair in message.Headers)
        {
            writer.WriteString(pair.Key.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
            writer.WriteString(pair.Value.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
        }

        writer.Write(message.Data.Length);
        writer.Write(message.Data.AsSpan());
    }

    public static OutgoingMessage ToOutgoingMessage(this ReadOnlySpan<byte> buffer)
    {
        using var owner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        var reader = owner.ReaderFor(buffer);
        return reader.ReadOutgoingMessage();
    }

    private static SequenceReader ReaderFor(this IMemoryOwner<byte> owner, ReadOnlySpan<byte> buffer)
    {
        var memory = owner.Memory;
        buffer.CopyTo(memory.Span);
        var sequence = new ReadOnlySequence<byte>(memory);
        var reader = new SequenceReader(sequence);
        return reader;
    }

    public static TMessage ToMessage<TMessage>(this ReadOnlySpan<byte> buffer) where TMessage : Message, new()
    {
        using var owner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        var reader = owner.ReaderFor(buffer);
        return reader.ReadMessage<TMessage>();
    }

    public static IEnumerable<Message> ToMessages(this ReadOnlySequence<byte> bytes)
    {
        var reader = new SequenceReader(bytes);
        var numberOfMessages = reader.ReadInt32(true);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var msg = ToMessage<Message>(ref reader);
            yield return msg;
        }
    }

    private static TMessage ToMessage<TMessage>(ref SequenceReader reader) where TMessage : Message, new()
    {
        var rawSequence = reader.RemainingSequence;
        var start = reader.Position;
        var msg = reader.ReadMessage<TMessage>();
        var end = reader.Position;
        //Capture the byte window and directly store to avoid duplication of serialization
        var messageFrame = rawSequence.Slice(start, end);
        msg.Bytes = messageFrame;
        return msg;
    }

    public static OutgoingMessage ReadOutgoingMessage(this ref SequenceReader reader)
    {
        var msg = reader.ReadMessage<OutgoingMessage>();
        msg.Destination = new Uri(reader.ReadString(LengthFormat.Compressed, Encoding.UTF8));
        var hasDeliverBy = reader.Read<bool>();
        if (hasDeliverBy)
            msg.DeliverBy = DateTime.FromBinary(reader.ReadInt64(true));
        var hasMaxAttempts = reader.Read<bool>();
        if (hasMaxAttempts)
            msg.MaxAttempts = reader.ReadInt32(true);
        return msg;
    }

    public static TMessage ReadMessage<TMessage>(this ref SequenceReader reader) where TMessage : Message, new()
    {
        var rawSequence = reader.RemainingSequence;
        var start = reader.Position;
        var msg = new TMessage
        {
            Id = new MessageId
            {
                SourceInstanceId = reader.Read<Guid>(),
                MessageIdentifier = reader.Read<Guid>()
            },
            Queue = reader.ReadString(LengthFormat.Compressed, Encoding.UTF8),
            SubQueue = reader.ReadString(LengthFormat.Compressed, Encoding.UTF8),
            SentAt = DateTime.FromBinary(reader.ReadInt64(true))
        };
        var headerCount = reader.ReadInt32(true);
        for (var i = 0; i < headerCount; ++i)
        {
            msg.Headers.Add(
                reader.ReadString(LengthFormat.Compressed, Encoding.UTF8),
                reader.ReadString(LengthFormat.Compressed, Encoding.UTF8)
            );
        }

        var dataLength = reader.ReadInt32(true);
        msg.Data = reader.RemainingSequence.Slice(reader.Position, dataLength).ToArray();
        reader.Skip(dataLength);
        var end = reader.Position;
        msg.Bytes = rawSequence.Slice(start, end);
        return msg;
    }

    public static ReadOnlyMemory<byte> AsReadOnlyMemory(this List<OutgoingMessage> messages)
    {
        using var writer = new PooledBufferWriter<byte>();
        writer.WriteOutgoingMessages(messages);
        return writer.WrittenMemory;
    }
        
    public static ReadOnlySpan<byte> AsSpan(this Message message)
    {
        if (message.Bytes.Length > 0)
        {
            return message.Bytes.FirstSpan;
        }
        using var writer = new PooledBufferWriter<byte>();
        WriteMessage(writer, message);
        return writer.WrittenMemory.Span;
    }

    public static ReadOnlyMemory<byte> AsReadOnlyMemory(this OutgoingMessage message)
    {
        using var writer = new PooledBufferWriter<byte>();
        WriteMessage(writer, message);
        writer.WriteString(message.Destination.OriginalString, Encoding.UTF8, LengthFormat.Compressed);
        writer.Write(message.DeliverBy.HasValue);
        if (message.DeliverBy.HasValue)
        {
            writer.Write(message.DeliverBy.Value.ToBinary());
        }
        writer.Write(message.MaxAttempts.HasValue);
        if (message.MaxAttempts.HasValue)
        {
            writer.Write(message.MaxAttempts.Value);
        }

        var messageBytes = new ReadOnlySequence<byte>(writer.WrittenMemory);
        message.Bytes = messageBytes;
        return writer.WrittenMemory;
    }
}