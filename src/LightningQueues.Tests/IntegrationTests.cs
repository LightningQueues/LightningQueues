using System;
using System.Linq;
using System.Threading.Tasks;
using LightningQueues.Logging;
using Shouldly;

namespace LightningQueues.Tests;

public class IntegrationTests : TestBase
{
    public async Task can_send_and_receive_after_queue_not_found()
    {
        await QueueScenario(async (receiver, token) =>
        {
            var senderLogger = new RecordingLogger();
            using var sender = new QueueConfiguration()
                .WithDefaultsForTest()
                .LogWith(senderLogger)
                .BuildAndStartQueue("sender");
            var message = NewMessage("receiver2");
            message.Destination = new Uri($"lq.tcp://localhost:{receiver.Endpoint.Port}");
            sender.Send(message);
            await Task.Delay(500, token); //passing buffer delay
            message.Queue = "receiver";
            sender.Send(message);
            var received = await receiver.Receive("receiver", token)
                .FirstAsync(token);
            received.ShouldNotBeNull();
            received.Message.Queue.ShouldBe(message.Queue);
            received.Message.Data.ShouldBe(message.Data);
            senderLogger.ErrorMessages.Any(x => x.Contains("Queue does not exist")).ShouldBeTrue();
        }, TimeSpan.FromSeconds(2), "receiver");
    }
}