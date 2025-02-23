using System;
using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests;

public class EncryptedTransportQueueTests : TestBase
{
    [Fact]
    public async Task can_send_and_receive_messages_over_TLS1_2()
    {
        await QueueScenario(config =>
        {
            config.WithSelfSignedCertificateSecurity();
        }, async (queue, token) =>
        {
            var message = NewMessage("test");
            message.Destination = new Uri($"lq.tcp://localhost:{queue.Endpoint.Port}");
            await Task.Delay(100, token);
            queue.Send(message);
            var received = await queue.Receive("test", token)
                .FirstAsync(token);
            received.ShouldNotBeNull();
            received.Message.Queue.ShouldBe(message.Queue);
            received.Message.Data.ShouldBe(message.Data);
        }, TimeSpan.FromSeconds(2));
    }
}