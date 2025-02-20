using System;
using System.Buffers;
using System.Collections.Generic;

namespace LightningQueues.Serialization;

public interface IMessageSerializer
{
    ReadOnlyMemory<byte> ToMemory(List<Message> messages);
    IList<Message> ReadMessages(ReadOnlySequence<byte> buffer);
    ReadOnlySpan<byte> AsSpan(Message message);
    Message ToMessage(ReadOnlySpan<byte> buffer);
}