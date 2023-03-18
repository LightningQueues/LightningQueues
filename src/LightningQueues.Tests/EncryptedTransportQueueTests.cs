using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests;

[Collection("SharedTestDirectory")]
public class EncryptedTransportQueueTests : IDisposable
{
    private readonly Queue _queue;
        
    public EncryptedTransportQueueTests(SharedTestDirectory testDirectory)
    {
        _queue = NewQueue(testDirectory.CreateNewDirectoryForTest(), secureTransport: true);
    }

    [Fact]
    public async Task can_send_and_receive_messages_over_TLS1_2()
    {
        var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var message = NewMessage<OutgoingMessage>("test");
        message.Destination = new Uri($"lq.tcp://localhost:{_queue.Endpoint.Port}");
        await Task.Delay(100, cancellation.Token);
        _queue.Send(message);
        var received = await _queue.Receive("test", cancellation.Token)
            .FirstAsync(cancellation.Token);
        received.ShouldNotBeNull();
        received.Message.Queue.ShouldBe(message.Queue);
        received.Message.Data.ShouldBe(message.Data);
        cancellation.Cancel();
    }

    public void Dispose()
    {
        _queue.Dispose();
        GC.SuppressFinalize(this);
    }
}