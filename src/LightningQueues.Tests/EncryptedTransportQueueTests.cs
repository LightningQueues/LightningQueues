using System;
using System.Linq;
using System.Threading.Tasks;
using Shouldly;

namespace LightningQueues.Tests;

public class EncryptedTransportQueueTests : TestBase
{
    public async Task can_send_and_receive_messages_over_TLS1_2()
    {
        await QueueScenario(config =>
        {
            config.WithSelfSignedCertificateSecurity();
        }, async (queue, token) =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            await DeterministicDelay(100, token);
            queue.Send(message);
            var received = await queue.Receive("test", cancellationToken: token)
                .FirstAsync(token);
            received.ShouldNotBeNull();
            received.Message.QueueString.ShouldBe(message.QueueString);
            received.Message.DataArray.ShouldBe(message.DataArray);
        }, TimeSpan.FromSeconds(5));
    }
}