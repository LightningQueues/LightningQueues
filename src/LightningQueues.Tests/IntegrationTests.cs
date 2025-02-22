using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using Shouldly;
using Xunit;
using static LightningQueues.Helpers.QueueBuilder;

namespace LightningQueues.Tests;

[Collection("SharedTestDirectory")]
public class IntegrationTests : IDisposable
{
    private readonly Queue _receiver;
    private readonly Queue _sender;
    private readonly RecordingLogger _senderLogger;

    public IntegrationTests(SharedTestDirectory testDirectory)
    {
        _senderLogger = new RecordingLogger();
        _receiver = NewQueue(testDirectory.CreateNewDirectoryForTest(), "receiver");
        _sender = NewQueue(testDirectory.CreateNewDirectoryForTest(), "sender", _senderLogger);
    }

    [Fact]
    public async Task can_send_and_receive_after_queue_not_found()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var message = NewMessage<Message>("receiver2");
        message.Destination = new Uri($"lq.tcp://localhost:{_receiver.Endpoint.Port}");
        _sender.Send(message);
        await Task.Delay(500, cancellation.Token); //passing buffer delay
        message.Queue = "receiver";
        _sender.Send(message);
        var received = await _receiver.Receive("receiver", cancellation.Token)
            .FirstAsync(cancellation.Token);
        received.ShouldNotBeNull();
        received.Message.Queue.ShouldBe(message.Queue);
        received.Message.Data.ShouldBe(message.Data);
        _senderLogger.ErrorMessages.Any(x => x.Contains("Queue does not exist")).ShouldBeTrue();
        await cancellation.CancelAsync();
    }

    public void Dispose()
    {
        using (_sender)
        using (_receiver)
            GC.SuppressFinalize(this);
    }
}