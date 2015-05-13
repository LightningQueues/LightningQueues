using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LightningQueues.Net.Protocol.V1;
using Should;
using Xunit;

namespace LightningQueues.Tests.Net.Protocol.V1
{
    public class SendingProtocolTests
    {
        readonly RecordingLogger _logger;
        readonly SendingProtocol _sender;

        public SendingProtocolTests()
        {
            _logger = new RecordingLogger();
            _sender = new SendingProtocol(_logger);
        }

        [Fact]
        public async Task writing_the_length()
        {
            var expected = 5;
            using (var ms = new MemoryStream())
            {
                await _sender.WriteLength(ms, 5);
                var actual = BitConverter.ToInt32(ms.ToArray(), 0);
                actual.ShouldEqual(expected);
            }
        }

        [Fact]
        public async Task writing_the_message_bytes()
        {
            var expected = new byte[] {1, 4, 6};
            using (var ms = new MemoryStream())
            {
                await _sender.WriteMessages(ms, expected);
                var actual = ms.ToArray();
                actual.SequenceEqual(expected).ShouldBeTrue();
            }
        }

        [Fact]
        public async Task receive_happy()
        {
            using (var ms = new MemoryStream())
            {
                ms.Write(Constants.ReceivedBuffer, 0, Constants.ReceivedBuffer.Length);
                ms.Position = 0;
                var result = await _sender.ReadReceived(ms);
                result.ShouldBeTrue();
            }
        }

        [Fact]
        public async Task receive_not_happy()
        {
            using (var ms = new MemoryStream())
            {
                var result = await _sender.ReadReceived(ms);
                result.ShouldBeFalse();
            }
        }

        [Fact]
        public async Task sending_acknowledgement()
        {
            using (var ms = new MemoryStream())
            {
                await _sender.WriteAcknowledge(ms);
                ms.ToArray().SequenceEqual(Constants.AcknowledgedBuffer).ShouldBeTrue();
            }
        }
    }
}