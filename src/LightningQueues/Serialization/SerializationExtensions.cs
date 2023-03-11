using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DotNext.Buffers;
using DotNext.IO;

namespace LightningQueues.Serialization;

public static class SerializationExtensions
{
    public static Message[] ToMessages(this byte[] buffer, bool wire = true)
    {
        using var ms = new MemoryStream(buffer);
        using var br = new BinaryReader(ms);
        if (wire) _ = br.ReadInt32(); //ignore payload length
        var numberOfMessages = br.ReadInt32();
        var msgs = new Message[numberOfMessages];
        for (var i = 0; i < numberOfMessages; i++)
        {
            msgs[i] = ReadSingleMessage<Message>(br);
        }
        return msgs;
    }

    public static Message ToMessage(this byte[] buffer)
    {
        using var ms = new MemoryStream(buffer);
        using var br = new BinaryReader(ms);
        return ReadSingleMessage<Message>(br);
    }

    public static OutgoingMessage ToOutgoingMessage(this byte[] buffer)
    {
        using var ms = new MemoryStream(buffer);
        using var br = new BinaryReader(ms);
        var msg = ReadSingleMessage<OutgoingMessage>(br);
        msg.Destination = new Uri(br.ReadString());
        var hasDeliverBy = br.ReadBoolean();
        if (hasDeliverBy)
            msg.DeliverBy = DateTime.FromBinary(br.ReadInt64());
        var hasMaxAttempts = br.ReadBoolean();
        if (hasMaxAttempts)
            msg.MaxAttempts = br.ReadInt32();

        return msg;
    }

    private static TMessage ReadSingleMessage<TMessage>(BinaryReader br) where TMessage : Message, new()
    {
        var msg = new TMessage
        {
            Id = new MessageId
            {
                SourceInstanceId = new Guid(br.ReadBytes(16)),
                MessageIdentifier = new Guid(br.ReadBytes(16))
            },
            Queue = br.ReadString(),
            SubQueue = br.ReadString(),
            SentAt = DateTime.FromBinary(br.ReadInt64())
        };
        var headerCount = br.ReadInt32();
        msg.Headers = new Dictionary<string, string>();
        for (var j = 0; j < headerCount; j++)
        {
            msg.Headers.Add(
                br.ReadString(),
                br.ReadString()
            );
        }
        var byteCount = br.ReadInt32();
        msg.Data = br.ReadBytes(byteCount);
        if (string.IsNullOrEmpty(msg.SubQueue))
            msg.SubQueue = null;

        return msg;
    }

    public static void WriteMessages(this IBufferWriter<byte> writer, IList<OutgoingMessage> messages)
    {
        writer.WriteInt32(messages.Count, true);
        Span<byte> id = stackalloc byte[16];
        foreach (var message in messages)
        {
            if (message.Bytes.Length == 0)
            {
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
            else
            {
                writer.Write(message.Bytes);
            }
        }
    }

    public static IEnumerable<Message> ToMessages(this ReadOnlySequence<byte> bytes)
    {
        var reader = new SequenceReader(bytes);
        var numberOfMessages = reader.ReadInt32(true);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var msg = ToMessage<Message>(ref reader, ref bytes);
            yield return msg;
        }
    }

    private static TMessage ToMessage<TMessage>(ref SequenceReader reader, ref ReadOnlySequence<byte> rawSequence)
        where TMessage : Message, new()
    {
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

    public static byte[] Serialize(this IList<OutgoingMessage> messages)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        writer.Write(messages.Count);
        foreach (var message in messages)
        {
            WriteSingleMessage(message, writer);
        }
        writer.Flush();
        return stream.ToArray();
    }
        
    public static byte[] Serialize(this Message message)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        WriteSingleMessage(message, writer);
        writer.Flush();
        return stream.ToArray();
    }

    public static byte[] Serialize(this OutgoingMessage message)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        WriteSingleMessage(message, writer);
        writer.Write(message.Destination.ToString());
        writer.Write(message.DeliverBy.HasValue);
        if(message.DeliverBy.HasValue)
            writer.Write(message.DeliverBy.Value.ToBinary());
        writer.Write(message.MaxAttempts.HasValue);
        if(message.MaxAttempts.HasValue)
            writer.Write(message.MaxAttempts.Value);
        writer.Flush();
        return stream.ToArray();
    }

    private static void WriteSingleMessage(Message message, BinaryWriter writer)
    {
        writer.Write(message.Id.SourceInstanceId.ToByteArray());
        writer.Write(message.Id.MessageIdentifier.ToByteArray());
        writer.Write(message.Queue);
        writer.Write(message.SubQueue ?? "");
        writer.Write(message.SentAt.ToBinary());

        writer.Write(message.Headers.Count);
        foreach (var pair in message.Headers)
        {
            writer.Write(pair.Key);
            writer.Write(pair.Value);
        }

        writer.Write(message.Data.Length);
        writer.Write(message.Data);
    }
}