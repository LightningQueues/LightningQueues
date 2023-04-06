using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using DotNext.Buffers;
using LightningQueues.Serialization;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests;

public class SerializationTests
{
    [Fact]
    public void can_serialize_and_deserialize_message_as_span()
    {
        var msg = NewMessage<OutgoingMessage>();
        msg.Destination = new Uri("lq.tcp://fake:1234");
        var msgs = new List<OutgoingMessage> { msg };
        using var writer = new PooledBufferWriter<byte>();
        writer.WriteOutgoingMessages(msgs);
        var serialized = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var deserialized = serialized.ToMessages().First();
        Assert.Equal(msg.Id, deserialized.Id);
        Assert.Equal(msg.Data, deserialized.Data);
    }
}