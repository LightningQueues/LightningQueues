using System;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using Shouldly;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Tests;

public class QueueContextTests : TestBase
{
    public async Task successfully_received_removes_message_from_queue()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            receivedContext.QueueContext.SuccessfullyReceived();
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            var allMessages = store.PersistedIncoming("test").ToList();
            allMessages.Count.ShouldBe(0);
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task move_to_moves_message_to_another_queue()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("another");
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            receivedContext.QueueContext.MoveTo("another");
            receivedContext.QueueContext.CommitChanges();
            
            var movedMessage = await queue.Receive("another", cancellationToken: token).FirstAsync(token);
            movedMessage.Message.QueueString.ShouldBe("another");
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
        });
    }
    
    public async Task send_enqueues_outgoing_message()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("response");
            
            var receivedMessage = NewMessage("test");
            queue.Enqueue(receivedMessage);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var responseMessage = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "response",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            
            receivedContext.QueueContext.Send(responseMessage);
            receivedContext.QueueContext.CommitChanges();
            
            var sent = await queue.Receive("response", cancellationToken: token).FirstAsync(token);
            sent.Message.QueueString.ShouldBe("response");
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task receive_later_with_time_span_delays_processing()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            var messageId = receivedContext.Message.Id;
            
            receivedContext.QueueContext.ReceiveLater(TimeSpan.FromMilliseconds(800));
            receivedContext.QueueContext.SuccessfullyReceived(); 
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            await DeterministicDelay(1000, token);
            
            var delayedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            delayedMessage.Message.Id.ShouldBe(messageId);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task receive_later_with_date_time_offset_delays_processing()
    {
        await QueueScenario(async (queue, token) =>
        {
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            var messageId = receivedContext.Message.Id;
            
            var futureTime = DateTimeOffset.Now.AddMilliseconds(800);
            receivedContext.QueueContext.ReceiveLater(futureTime);
            receivedContext.QueueContext.SuccessfullyReceived(); 
            receivedContext.QueueContext.CommitChanges();
            
            var store = (LmdbMessageStore)queue.Store;
            store.PersistedIncoming("test").Any().ShouldBeFalse();
            
            await DeterministicDelay(1000, token);
            
            var delayedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            delayedMessage.Message.Id.ShouldBe(messageId);
        }, TimeSpan.FromSeconds(5));
    }
    
    public async Task enqueue_adds_message_to_queue()
    {
        await QueueScenario(async (queue, token) =>
        {
            var receivedMessage = NewMessage("test");
            queue.Enqueue(receivedMessage);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var newMessage = NewMessage("test", "new payload");
            var newMessageId = newMessage.Id;
            
            receivedContext.QueueContext.Enqueue(newMessage);
            
            receivedContext.QueueContext.SuccessfullyReceived();
            receivedContext.QueueContext.CommitChanges();
            
            var enqueuedMessage = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            enqueuedMessage.Message.Id.ShouldBe(newMessageId);
            Encoding.UTF8.GetString(enqueuedMessage.Message.DataArray!).ShouldBe("new payload");
        }, TimeSpan.FromSeconds(3));
    }
    
    public async Task commit_changes_executes_all_pending_actions()
    {
        await QueueScenario(async (queue, token) =>
        {
            queue.CreateQueue("response");
            
            var message = NewMessage("test");
            queue.Enqueue(message);
            
            var receivedContext = await queue.Receive("test", cancellationToken: token).FirstAsync(token);
            
            var messageToMove = NewMessage("test", "move me");
            var messageToSend = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "response",
                destinationUri: $"lq.tcp://localhost:{queue.Endpoint.Port}"
            );
            var messageToEnqueue = NewMessage("test", "enqueued");
            
            var originalMessageId = message.Id;
            var moveMessageId = messageToMove.Id;
            var sendMessageId = messageToSend.Id;
            var enqueueMessageId = messageToEnqueue.Id;
            
            receivedContext.QueueContext.SuccessfullyReceived(); 
            receivedContext.QueueContext.Enqueue(messageToMove); 
            receivedContext.QueueContext.Send(messageToSend);    
            receivedContext.QueueContext.Enqueue(messageToEnqueue); 
            
            receivedContext.QueueContext.CommitChanges();
            
            
            var store = (LmdbMessageStore)queue.Store;
            var originalStillExists = store.PersistedIncoming("test")
                .Any(m => m.Id == originalMessageId);
            originalStillExists.ShouldBeFalse();
            
            var enqueuedMessages = await queue.Receive("test", cancellationToken: token)
                .Take(2)
                .ToListAsync(token);
            
            enqueuedMessages.Count.ShouldBe(2);
            
            var foundIds = enqueuedMessages.Select(m => m.Message.Id).ToList();
            foundIds.ShouldContain(moveMessageId);
            foundIds.ShouldContain(enqueueMessageId);
            
            var sentMessage = await queue.Receive("response", cancellationToken: token).FirstAsync(token);
            sentMessage.Message.Id.ShouldBe(sendMessageId);
        }, TimeSpan.FromSeconds(5));
    }
}