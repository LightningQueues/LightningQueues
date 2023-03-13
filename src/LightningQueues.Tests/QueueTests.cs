using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Builders;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests;

[Collection("SharedTestDirectory")]
public class QueueTests : IDisposable
{
    private readonly SharedTestDirectory _testDirectory;
    private readonly Queue _queue;

    public QueueTests(SharedTestDirectory testDirectory)
    {
        _testDirectory = testDirectory;
        _queue = NewQueue(testDirectory.CreateNewDirectoryForTest());
    }

    [Fact]
    public async ValueTask receive_at_a_later_time()
    {
        _queue.ReceiveLater(new Message { Queue = "test", Id = MessageId.GenerateRandom() }, TimeSpan.FromSeconds(1));
        var receiveTask = _queue.Receive("test").FirstAsync();
        await Task.WhenAny(receiveTask.AsTask(), Task.Delay(100));
        receiveTask.IsCompleted.ShouldBeFalse();
        await Task.WhenAny(receiveTask.AsTask(), Task.Delay(1000));
        receiveTask.IsCompleted.ShouldBeTrue();
    }

    [Fact]
    public async ValueTask receive_at_a_specified_time()
    {
        var time = DateTimeOffset.Now.AddSeconds(1);
        _queue.ReceiveLater(new Message { Queue = "test", Id = MessageId.GenerateRandom() }, time);
        var receiveTask = _queue.Receive("test").FirstAsync();
        await Task.WhenAny(receiveTask.AsTask(), Task.Delay(100));
        receiveTask.IsCompleted.ShouldBeFalse();
        await Task.WhenAny(receiveTask.AsTask(), Task.Delay(900));
        receiveTask.IsCompleted.ShouldBeTrue();
    }

    [Fact]
    public async ValueTask enqueue_a_message()
    {
        var expected = NewMessage<Message>("test");
        var receiveTask = _queue.Receive("test").FirstAsync();
        _queue.Enqueue(expected);
        var result = await receiveTask;
        expected.ShouldBeSameAs(result.Message);
    }

    [Fact]
    public async ValueTask moving_queues()
    {
        _queue.CreateQueue("another");
        var expected = NewMessage<Message>("test");
        var anotherTask = _queue.Receive("another").FirstAsync(); //.ToObservable().Subscribe(x => afterMove = x.Message))
        var testTask = _queue.Receive("test").FirstAsync(); //.ToObservable().Subscribe(x => first = x.Message))
        _queue.Enqueue(expected);
        var first = await testTask;
        _queue.MoveToQueue("another", first.Message);

        var afterMove = await anotherTask;
        afterMove.Message.Queue.ShouldBe("another");
    }

    [Fact]
    public async Task send_message_to_self()
    {
        var message = NewMessage<OutgoingMessage>("test");
        message.Destination = new Uri($"lq.tcp://localhost:{_queue.Endpoint.Port}");
        _queue.Send(message);
        var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var received = await _queue.Receive("test").FirstAsync(cancellation.Token);
        received.ShouldNotBeNull();
        received.Message.Queue.ShouldBe(message.Queue);
        received.Message.Data.ShouldBe(message.Data);
    }

    [Fact]
    public async Task sending_to_bad_endpoint_no_retries_integration_test()
    {
        using var queue = NewQueue(_testDirectory.CreateNewDirectoryForTest(), timeoutAfter: TimeSpan.FromSeconds(1));
        var message = NewMessage<OutgoingMessage>("test");
        message.MaxAttempts = 1;
        message.Destination = new Uri($"lq.tcp://boom:{queue.Endpoint.Port + 1}");
        queue.Send(message);
        await Task.Delay(1000); //connect timeout cancellation
        var store = (LmdbMessageStore) queue.Store;
        store.PersistedOutgoingMessages().Any().ShouldBeFalse();
    }

    [Fact]
    public void can_start_two_instances_for_IIS_stop_and_start()
    {
        //This shows that the port doesn't have an exclusive lock, and that lmdb itself can have multiple instances
        var path = _testDirectory.CreateNewDirectoryForTest();
        var store = new LmdbMessageStore(path);
        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.LogWith(new RecordingLogger());
        queueConfiguration.AutomaticEndpoint();
        queueConfiguration.StoreMessagesWith(store);
        using var queue = queueConfiguration.BuildQueue();
        using var queue2 = queueConfiguration.BuildQueue();
        queue.CreateQueue("test");
        queue.Start();
        queue2.CreateQueue("test");
        queue2.Start();
        var msgs = queue.Receive("test");
        var msgs2 = queue2.Receive("test");
        msgs.ShouldNotBeNull();
        msgs2.ShouldNotBeNull();
    }

    public void Dispose()
    {
        _queue.Dispose();
        GC.SuppressFinalize(this);
    }
}