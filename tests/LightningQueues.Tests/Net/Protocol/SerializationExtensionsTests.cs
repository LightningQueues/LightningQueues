using System.Linq;
using System.Text;
using LightningQueues.Net.Protocol.V1;
using Should;
using Xunit;

namespace LightningQueues.Tests.Net.Protocol
{
    public class SerializationExtensionsTests
    {
        [Fact]
        public void can_serialize_and_deserialize()
        {
            var expected = new IncomingMessage
            {
                Data = Encoding.UTF8.GetBytes("hello"),
                Id = MessageId.GenerateRandom(),
                Queue = "queue",
            };
            var messagesBytes = new[] {expected}.Serialize();
            var actual = messagesBytes.ToMessages().First();

            actual.Queue.ShouldEqual(expected.Queue);
            actual.Data.ShouldEqual(expected.Data);
            actual.Id.ShouldEqual(expected.Id);
        }
    }
}