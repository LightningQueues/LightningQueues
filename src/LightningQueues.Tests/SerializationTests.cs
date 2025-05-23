using System;
using System.Buffers;
using System.Collections.Generic;
using LightningQueues.Serialization;
using Shouldly;

namespace LightningQueues.Tests;

public class SerializationTests : TestBase
{
    public void can_serialize_and_deserialize_message_as_span()
    {
        var serializer = new MessageSerializer();
        var msg = Message.Create(
            data: "hello"u8.ToArray(),
            queue: "test",
            destinationUri: "lq.tcp://fake:1234"
        );
        var msgs = new List<Message> { msg };
        var memory = serializer.ToMemory(msgs);
        var serialized = new ReadOnlySequence<byte>(memory);
        var deserialized = serializer.ToMessage(serialized.Slice(sizeof(int)).FirstSpan);
        deserialized.Id.ShouldBe(msg.Id);
        deserialized.DataArray.ShouldBe(msg.DataArray);
    }
}

