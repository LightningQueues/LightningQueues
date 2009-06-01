using System;
using System.Net;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues
{
    /// <summary>
    /// 
    /// </summary>
    public interface IQueueManager : IDisposable
    {
        int NumberOfReceivedMessagesToKeep { get; set; }
        int? NumberOfMessagesToKeepInProcessedQueues { get; set; }
        int? NumberOfMessagesToKeepOutgoingQueues { get; set; }
        TimeSpan? OldestMessageInProcessedQueues { get; set; }
        TimeSpan? OldestMessageInOutgoingQueues { get; set; }
        string Path { get; }
        IPEndPoint Endpoint { get; }
        string[] Queues { get; }
        event Action<Endpoint> FailedToSendMessagesTo;
        void WaitForAllMessagesToBeSent();
        IQueue GetQueue(string queue);
        PersistentMessage[] GetAllMessages(string queueName, string subqueue);
        HistoryMessage[] GetAllProcessedMessages(string queueName);
        PersistentMessageToSend[] GetAllSentMessages();
        PersistentMessageToSend[] GetMessagesCurrentlySending();
        Message Peek(string queueName);
        Message Peek(string queueName, TimeSpan timeout);
        Message Peek(string queueName, string subqueue);
        Message Peek(string queueName, string subqueue, TimeSpan timeout);
        Message Receive(string queueName);
        Message Receive(string queueName, TimeSpan timeout);
        Message Receive(string queueName, string subqueue);
        Message Receive(string queueName, string subqueue, TimeSpan timeout);
        MessageId Send(Uri uri, MessagePayload payload);
        void CreateQueues(params string[] queueNames);
        void MoveTo(string subqueue, Message message);
        void EnqueueDirectlyTo(string queue, string subqueue, MessagePayload payload);
        PersistentMessage PeekById(string queueName, MessageId id);
        string[] GetSubqueues(string queueName);
        int GetNumberOfMessages(string queueName);
        void FailedToSendTo(Endpoint endpointThatWeFailedToSendTo);
    }
}
