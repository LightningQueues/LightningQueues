using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Storage;
using Xunit;
using static LightningQueues.Helpers.QueueBuilder;

namespace LightningQueues.Tests.Storage;

public class NoStorageIntegrationTester : IAsyncDisposable
{
    private readonly Queue _sender;
    private readonly Queue _receiver;

    public NoStorageIntegrationTester()
    {
        _sender = NewQueue(store:new NoStorage());
        _receiver = NewQueue(store:new NoStorage());
    }

    [Fact]
    public async Task can_send_and_receive_without_storage()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var receiveTask = _receiver.Receive("test", cancellation.Token).FirstAsync(cancellation.Token);

        var destination = new Uri($"lq.tcp://localhost:{_receiver.Endpoint.Port}");
        var message = NewMessage<Message>("test");
        message.Destination = destination;
        _sender.Send(message);
        await receiveTask;
        await cancellation.CancelAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _sender.DisposeAsync();
        await _receiver.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}