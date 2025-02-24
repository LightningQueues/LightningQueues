using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests;

public class QueueTests : TestBase
{
    public async Task receive_at_a_later_time()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.ReceiveLater(NewMessage("test"), TimeSpan.FromSeconds(1));
            var receiveTask = queue.Receive("test", token).FirstAsync(token);
            await Task.Delay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await Task.WhenAny(receiveTask.AsTask(), Task.Delay(1000, token));
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(2));
    }

    public async Task receive_at_a_specified_time()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.ReceiveLater(NewMessage("test"), DateTimeOffset.Now.AddSeconds(1));
            var receiveTask = queue.Receive("test", token).FirstAsync(token);
            await Task.Delay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await Task.WhenAny(receiveTask.AsTask(), Task.Delay(TimeSpan.FromSeconds(1), token));
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(2));
    }

    public async Task enqueue_a_message()
    {
        await QueueScenario(async (queue, token) =>
        {
            var expected = NewMessage("test");
            var receiveTask = queue.Receive("test", token).FirstAsync(token);
            queue.Enqueue(expected);
            var result = await receiveTask;
            expected.ShouldBeSameAs(result.Message);
        });
    }

    public async Task moving_queues()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("another");
            var expected = NewMessage("test");
            queue.Enqueue(expected);
            var message = await queue.Receive("test", token).FirstAsync(token);
            queue.MoveToQueue("another", message.Message);

            message = await queue.Receive("another", token).FirstAsync(token);
            message.Message.Queue.ShouldBe("another");
        });
    }

    public async Task send_message_to_self()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = NewMessage("test");
            message.Destination = new Uri($"lq.tcp://localhost:{queue.Endpoint.Port}");
            queue.Send(message);
            var received = await queue.Receive("test", token).FirstAsync(token);
            received.ShouldNotBeNull();
            received.Message.Queue.ShouldBe(message.Queue);
            received.Message.Data.ShouldBe(message.Data);
        });
    }

    public async Task sending_to_bad_endpoint_no_retries_integration_test()
    {
        await QueueScenario(config =>
            {
                config.TimeoutNetworkBatchAfter(TimeSpan.FromSeconds(1));
            },
            async (queue, token) =>
            {
                var message = NewMessage("test");
                message.MaxAttempts = 1;
                message.Destination = new Uri($"lq.tcp://boom:{queue.Endpoint.Port + 1}");
                queue.Send(message);
                await Task.Delay(5000, token); //connect timeout cancellation, but windows is slow
                var store = (LmdbMessageStore)queue.Store;
                store.PersistedOutgoing().Any().ShouldBeFalse();
            }, TimeSpan.FromSeconds(10));
    }

    public async Task can_start_two_instances_for_IIS_stop_and_start()
    {
        //This shows that the port doesn't have an exclusive lock, and that lmdb itself can have multiple instances
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var serializer = new MessageSerializer();
        using var store = new LmdbMessageStore(TempPath(), serializer);
        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.LogWith(new RecordingLogger(Console));
        queueConfiguration.AutomaticEndpoint();
        queueConfiguration.SerializeWith(serializer);
        queueConfiguration.StoreMessagesWith(store);
        using var queue = queueConfiguration.BuildQueue();
        using var queue2 = queueConfiguration.BuildQueue();
        queue.CreateQueue("test");
        queue.Start();
        queue2.CreateQueue("test");
        queue2.Start();
        await cancellation.CancelAsync();
    }
}