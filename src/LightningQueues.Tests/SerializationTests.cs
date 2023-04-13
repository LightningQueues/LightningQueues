using System;
using System.Buffers;
using System.Collections.Generic;
using LightningQueues.Serialization;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests;

public class SerializationTests
{
    [Fact]
    public void can_serialize_and_deserialize_message_as_span()
    {
        var serializer = new MessageSerializer();
        var msg = NewMessage<Message>();
        msg.Destination = new Uri("lq.tcp://fake:1234");
        var msgs = new List<Message> { msg };
        var memory = serializer.ToMemory(msgs);
        var serialized = new ReadOnlySequence<byte>(memory);
        var deserialized = serializer.ToMessage(serialized.Slice(sizeof(int)).FirstSpan);
        Assert.Equal(msg.Id, deserialized.Id);
        Assert.Equal(msg.Data, deserialized.Data);
    }
    
}

