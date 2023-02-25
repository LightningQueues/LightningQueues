using System.Linq;
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
            Queue = "queue"
        };
        var messagesBytes = new[] {expected}.Serialize();
        var actual = messagesBytes.ToMessages().First();

        actual.Queue.ShouldBe(expected.Queue);
        actual.Data.ShouldBe(expected.Data);
        actual.Id.ShouldBe(expected.Id);
    }
}