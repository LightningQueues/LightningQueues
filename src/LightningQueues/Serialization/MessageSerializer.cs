using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using DotNext.Buffers;
using DotNext.IO;

namespace LightningQueues.Serialization;

public class MessageSerializer : IMessageSerializer
{
    private readonly List<CommonString> _queueNames = new();
    private readonly List<CommonString> _subQueueNames = new();
    private readonly List<CommonString> _headerKeys = new();
    private readonly List<CommonString> _uris = new();
    
    public ReadOnlyMemory<byte> ToMemory(List<Message> messages)
    {
        using var writer = new PooledBufferWriter<byte>();
        WriteMessages(writer, messages);
        return writer.WrittenMemory;
    }

    public IEnumerable<Message> ReadMessages(ReadOnlySequence<byte> buffer)
    {
        var reader = new SequenceReader(buffer);
        var numberOfMessages = reader.ReadInt32(true);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var msg = ReadMessage(ref reader);
            yield return msg;
        }
    }
    
    private void WriteMessages(IBufferWriter<byte> writer, List<Message> messages)
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
        }
    }

    private static void WriteMessage(IBufferWriter<byte> writer, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.SourceInstanceId.TryWriteBytes(id);
        writer.Write(id);
        message.Id.MessageIdentifier.TryWriteBytes(id);
        writer.Write(id);
        writer.WriteString(message.Queue, Encoding.UTF8, LengthFormat.Compressed);
        writer.WriteString(message.SubQueue ?? string.Empty, Encoding.UTF8, LengthFormat.Compressed);
        writer.Write(message.SentAt.ToBinary());

        writer.Write(message.Headers.Count);
        foreach (var pair in message.Headers)
        {
            writer.WriteString(pair.Key.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
            writer.WriteString(pair.Value.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
        }

        writer.Write(message.Data.Length);
        writer.Write(message.Data.AsSpan());

        writer.WriteString(message.Destination?.OriginalString ?? string.Empty, Encoding.UTF8, LengthFormat.Compressed);
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
        using var owner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        var reader = ReaderFor(owner, buffer);
        return ReadMessage(ref reader);
    }

    private Message ReadMessage(ref SequenceReader reader)
    {
        var rawSequence = reader.RemainingSequence;
        var start = reader.Position;
        var msg = new Message
        {
            Id = new MessageId
            {
                SourceInstanceId = ReadGuid(ref reader),
                MessageIdentifier = ReadGuid(ref reader)
            },
            Queue = ReadCommonString(ref reader, _queueNames),
            SubQueue = ReadCommonString(ref reader, _subQueueNames),
            SentAt = DateTime.FromBinary(reader.ReadInt64(true))
        };
        var headerCount = reader.ReadInt32(true);
        for (var i = 0; i < headerCount; ++i)
        {
            msg.Headers.Add(
                ReadCommonString(ref reader, _headerKeys),
                reader.ReadString(LengthFormat.Compressed, Encoding.UTF8)
            );
        }

        var dataLength = reader.ReadInt32(true);
        msg.Data = reader.RemainingSequence.Slice(reader.Position, dataLength).ToArray();
        reader.Skip(dataLength);
        var uri = ReadCommonString(ref reader, _uris);
        if(!string.IsNullOrEmpty(uri))
            msg.Destination = new Uri(uri);
        var hasDeliverBy = reader.Read<bool>();
        if (hasDeliverBy)
            msg.DeliverBy = DateTime.FromBinary(reader.ReadInt64(true));
        var hasMaxAttempts = reader.Read<bool>();
        if (hasMaxAttempts)
            msg.MaxAttempts = reader.ReadInt32(true);
        var end = reader.Position;
        msg.Bytes = rawSequence.Slice(start, end);
        return msg;
    }

    private static Guid ReadGuid(ref SequenceReader reader)
    {
        Span<byte> guidBuffer = stackalloc byte[16];
        reader.Read(guidBuffer);
        return new Guid(guidBuffer);
    }

    public ReadOnlySpan<byte> AsSpan(Message message)
    {
        if (message.Bytes.Length > 0)
        {
            return message.Bytes.FirstSpan;
        }
        using var writer = new PooledBufferWriter<byte>();
        WriteMessage(writer, message);
        return writer.WrittenMemory.Span;
    }

    private readonly ReaderWriterLockSlim _lock = new();
    private string ReadCommonString(ref SequenceReader reader, List<CommonString> checkList)
    {
        try
        {
            _lock.EnterReadLock();
            foreach (var item in checkList)
            {
                var bytes = item.Bytes;
                if (reader.RemainingSequence.Length < bytes.Length)
                    continue;


                var possibleMatch = reader.RemainingSequence.Slice(0, bytes.Length);
                if (!bytes.FirstSpan.SequenceEqual(possibleMatch.FirstSpan))
                    continue;

                reader.Skip((int)bytes.Length);
                return item.Value;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        var remaining = reader.RemainingSequence;
        var start = reader.Position;
        var stringValue = reader.ReadString(LengthFormat.Compressed, Encoding.UTF8);
        var end = reader.Position;
        var length = end.GetInteger() - start.GetInteger();
        try
        {
            _lock.EnterWriteLock();
            checkList.Add(new CommonString(stringValue, remaining.Slice(0, length)));
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        return stringValue;
    }
}