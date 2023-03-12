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
        var expected = new OutgoingMessage
        {
            Data = "hello"u8.ToArray(),
            Id = MessageId.GenerateRandom(),
            Queue = "queue",
            Destination = new Uri("lq.tcp://fake:1234")
        };
        var messagesBytes = expected.AsReadOnlyMemory();
        var actual = messagesBytes.Span.ToMessage<OutgoingMessage>();

        actual.Queue.ShouldBe(expected.Queue);
        actual.Data.ShouldBe(expected.Data);
        actual.Id.ShouldBe(expected.Id);
    }
}