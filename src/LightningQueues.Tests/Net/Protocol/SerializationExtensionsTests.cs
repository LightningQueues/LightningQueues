using System;
using LightningQueues.Serialization;
using Shouldly;

namespace LightningQueues.Tests.Net.Protocol;

public class SerializationExtensionsTests
{
    public void can_serialize_and_deserialize()
    {
        var serializer = new MessageSerializer();
        var expected = Message.Create(
            data: "hello"u8.ToArray(),
            queue: "queue",
            destinationUri: "lq.tcp://fake:1234"
        );
        var messageBytes = serializer.AsSpan(expected);
        var actual = serializer.ToMessage(messageBytes);
        
        actual.QueueString.ShouldBe(expected.QueueString);
        actual.DataArray.ShouldBe(expected.DataArray);
        actual.Id.ShouldBe(expected.Id);
        actual.Destination.ShouldBe(expected.Destination);
        actual.SentAt.ShouldBe(expected.SentAt);
    }
}