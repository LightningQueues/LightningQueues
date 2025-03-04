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
            await DeterministicDelay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await DeterministicDelay(2000, token);
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(4));
    }

    public async Task receive_at_a_specified_time()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.ReceiveLater(NewMessage("test"), DateTimeOffset.Now.AddSeconds(1));
            var receiveTask = queue.Receive("test", token).FirstAsync(token);
            await DeterministicDelay(100, token);
            receiveTask.IsCompleted.ShouldBeFalse();
            await DeterministicDelay(2000, token);
            receiveTask.IsCompleted.ShouldBeTrue();
        }, TimeSpan.FromSeconds(4));
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
                await DeterministicDelay(5000, token); //connect timeout cancellation, but windows is slow
                var store = (LmdbMessageStore)queue.Store;
                store.PersistedOutgoing().Any().ShouldBeFalse();
            }, TimeSpan.FromSeconds(10));
    }

    public async Task can_start_two_instances_for_IIS_stop_and_start()
    {
        //This shows that the port doesn't have an exclusive lock, and that lmdb itself can have multiple instances
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.LogWith(new RecordingLogger(Console));
        queueConfiguration.AutomaticEndpoint();
        queueConfiguration.SerializeWith(serializer);
        queueConfiguration.StoreMessagesWith(() => store);
        using var queue = queueConfiguration.BuildQueue();
        using var queue2 = queueConfiguration.BuildQueue();
        queue.CreateQueue("test");
        queue.Start();
        queue2.CreateQueue("test");
        queue2.Start();
        await cancellation.CancelAsync();
    }
    
    public async Task send_batch_of_messages()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Create batch of messages for self
            var message1 = NewMessage("test", "payload1");
            var message2 = NewMessage("test", "payload2");
            var message3 = NewMessage("test", "payload3");
            
            // Set destination to self for all messages
            var destination = new Uri($"lq.tcp://localhost:{queue.Endpoint.Port}");
            message1.Destination = destination;
            message2.Destination = destination;
            message3.Destination = destination;
            
            // Send all messages as a batch
            queue.Send(message1, message2, message3);
            
            // Receive all the messages (should be 3)
            var receivedMessages = await queue.Receive("test", token)
                .Take(3)
                .ToListAsync(token);
            
            receivedMessages.Count.ShouldBe(3);
            
            // Verify we received each message
            var payloads = receivedMessages
                .Select(ctx => System.Text.Encoding.UTF8.GetString(ctx.Message.Data))
                .ToList();
                
            payloads.ShouldContain("payload1");
            payloads.ShouldContain("payload2");
            payloads.ShouldContain("payload3");
            await DeterministicDelay(TimeSpan.FromSeconds(1), token);
            
            // Verify the message store shows they were all sent
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedOutgoing().Count().ShouldBe(0); // Should be 0 since they were processed
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_from_multiple_queues_concurrently()
    {
        await QueueScenario(async (queue, token) =>
        {
            // Create multiple queues
            queue.CreateQueue("queue1");
            queue.CreateQueue("queue2");
            queue.CreateQueue("queue3");
            
            // Enqueue messages in different queues
            var message1 = NewMessage("queue1", "payload1");
            var message2 = NewMessage("queue2", "payload2");
            var message3 = NewMessage("queue3", "payload3");
            
            queue.Enqueue(message1);
            queue.Enqueue(message2);
            queue.Enqueue(message3);
            
            // Start receiving from all queues concurrently
            var receiveQueue1Task = queue.Receive("queue1", token).FirstAsync(token);
            var receiveQueue2Task = queue.Receive("queue2", token).FirstAsync(token);
            var receiveQueue3Task = queue.Receive("queue3", token).FirstAsync(token);
            
            // Wait for all receives to complete and get results
            var received1 = await receiveQueue1Task;
            var received2 = await receiveQueue2Task;
            var received3 = await receiveQueue3Task;
            
            System.Text.Encoding.UTF8.GetString(received1.Message.Data).ShouldBe("payload1");
            System.Text.Encoding.UTF8.GetString(received2.Message.Data).ShouldBe("payload2");
            System.Text.Encoding.UTF8.GetString(received3.Message.Data).ShouldBe("payload3");
            
            received1.Message.Queue.ShouldBe("queue1");
            received2.Message.Queue.ShouldBe("queue2");
            received3.Message.Queue.ShouldBe("queue3");
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task get_all_queue_names()
    {
        await QueueScenario((queue, token) =>
        {
            // Create multiple queues
            queue.CreateQueue("queue1");
            queue.CreateQueue("queue2");
            queue.CreateQueue("queue3");
            
            // Get all queue names
            var queueNames = queue.Queues;
            
            // Verify all queues are listed (including the default "test" queue)
            queueNames.Length.ShouldBe(4); 
            queueNames.ShouldContain("test");
            queueNames.ShouldContain("queue1");
            queueNames.ShouldContain("queue2");
            queueNames.ShouldContain("queue3");
            
            return Task.CompletedTask;
        });
    }
}