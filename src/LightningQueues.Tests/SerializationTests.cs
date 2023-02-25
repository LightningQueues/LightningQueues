using System.Linq;
using LightningQueues.Serialization;
using Xunit;

namespace LightningQueues.Tests;

public class SerializationTests
{
    [Fact]
    public void can_serialize_and_deserialize_message_as_span()
    {
        var msg = ObjectMother.NewMessage<OutgoingMessage>();
        var msgs = new [] { msg };
        var serialized = msgs.AsReadonlySequence();
        var deserialized = serialized.ToMessages().First();
        Assert.Equal(msg.Id, deserialized.Id);
        Assert.Equal(msg.Data, deserialized.Data);
    }
}