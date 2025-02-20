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
    private static MemStringEqualityComparer comp = new();
    ConcurrentDictionary<ReadOnlyMemory<char>, string> _commonStrings = new(comp);
    
    public ReadOnlyMemory<byte> ToMemory(List<Message> messages)
    {
        using var writer = new PoolingBufferWriter<byte>(ArrayPool<byte>.Shared.ToAllocator());
        WriteMessages(writer, messages);
        return writer.WrittenMemory;
    }

    public IList<Message> ReadMessages(ReadOnlySequence<byte> buffer)
    {
        var messages = new List<Message>();
        var reader = new SequenceReader(buffer);
        var numberOfMessages = reader.ReadLittleEndian<int>();
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
        writer.Encode(message.Queue, Encoding.UTF8, LengthFormat.Compressed);
        writer.Encode(message.SubQueue ?? string.Empty, Encoding.UTF8, LengthFormat.Compressed);
        writer.WriteLittleEndian(message.SentAt.ToBinary());
        

        writer.WriteLittleEndian(message.Headers.Count);
        foreach (var pair in message.Headers)
        {
            writer.Encode(pair.Key.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
            writer.Encode(pair.Value.AsSpan(), Encoding.UTF8, LengthFormat.Compressed);
        }

        writer.WriteLittleEndian(message.Data.Length);
        writer.Write(message.Data.AsSpan());

        writer.Encode(message.Destination?.OriginalString ?? string.Empty, Encoding.UTF8, LengthFormat.Compressed);
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
            Queue = ReadCommonString(ref reader),
            SubQueue = ReadCommonString(ref reader),
            SentAt = DateTime.FromBinary(reader.ReadLittleEndian<long>())
        };
        
        var headerCount = reader.ReadLittleEndian<int>();
        for (var i = 0; i < headerCount; ++i)
        {
            msg.Headers.Add(
                ReadCommonString(ref reader),
                reader.Decode(Encoding.UTF8, LengthFormat.Compressed).ToString()
            );
        }

        var dataLength = reader.ReadLittleEndian<int>();
        msg.Data = reader.RemainingSequence.Slice(reader.Position, dataLength).ToArray();
        reader.Skip(dataLength);
        var uri = ReadCommonString(ref reader);
        if (!string.IsNullOrEmpty(uri))
            msg.Destination = new Uri(uri);
        var hasDeliverBy = Convert.ToBoolean(reader.ReadLittleEndian<int>());
        if (hasDeliverBy)
        {
            msg.DeliverBy = DateTime.FromBinary(reader.ReadLittleEndian<long>()).ToLocalTime();
        }

        var hasMaxAttempts = Convert.ToBoolean(reader.ReadLittleEndian<int>());
        if (hasMaxAttempts)
        {
            msg.MaxAttempts = reader.ReadLittleEndian<int>();
        }

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

        using var writer = new PoolingBufferWriter<byte>(ArrayPool<byte>.Shared.ToAllocator());
        WriteMessage(writer, message);
        return writer.WrittenMemory.Span;
    }

    private string ReadCommonString(ref SequenceReader reader)
    {
        var stringBlock = reader.Decode(Encoding.UTF8, LengthFormat.Compressed);
        if (_commonStrings.TryGetValue(stringBlock.Memory, out var commonString))
        {
            return commonString;
        }
        var returnValue = stringBlock.ToString();
        _commonStrings[stringBlock.Memory] = returnValue;
        return returnValue;
    }
}