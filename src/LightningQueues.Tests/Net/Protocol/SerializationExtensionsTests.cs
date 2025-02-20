using System;
using LightningQueues.Serialization;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests.Net.Protocol;

public class SerializationExtensionsTests
{
    [Fact]
    public void can_serialize_and_deserialize()
    {
        var serializer = new MessageSerializer();
        var expected = new Message
        {
            Data = "hello"u8.ToArray(),
            Id = MessageId.GenerateRandom(),
            Queue = "queue",
            Destination = new Uri("lq.tcp://fake:1234"),
        };
        var messageBytes = serializer.AsSpan(expected);
        var actual = serializer.ToMessage(messageBytes);
        
        actual.Queue.ShouldBe(expected.Queue);
        actual.Data.ShouldBe(expected.Data);
        actual.Id.ShouldBe(expected.Id);
        actual.Destination.ShouldBe(expected.Destination);
        actual.SentAt.ShouldBe(expected.SentAt);
    }
}