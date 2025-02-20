using System;
using System.Collections.Generic;
using System.Linq;

namespace LightningQueues.Storage;

public class NoStorage : IMessageStore
{
    private readonly List<string> _queues = new();

    private class NoStorageTransaction : ITransaction
    {
        public void Commit()
        {
        }

        public void Dispose()
        {
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    public ITransaction BeginTransaction()
    {
        return new NoStorageTransaction();
    }

    public void CreateQueue(string queueName)
    {
        _queues.Add(queueName);
    }

    public void StoreIncomingMessage(Message message)
    {
    }

    public void StoreIncoming(IEnumerable<Message> messages)
    {
    }

    public void StoreIncomingMessage(ITransaction transaction, Message message)
    {
    }

    public void StoreIncoming(ITransaction transaction, IEnumerable<Message> messages)
    {
    }

    public void DeleteIncoming(IEnumerable<Message> messages)
    {
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        return Enumerable.Empty<Message>();
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        return Enumerable.Empty<Message>();
    }

    public void MoveToQueue(ITransaction transaction, string queueName, Message message)
    {
        message.Queue = queueName;
    }

    public void SuccessfullyReceived(ITransaction transaction, Message message)
    {
    }

    public void StoreOutgoing(ITransaction tx, Message message)
    {
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
    }

    public void StoreOutgoing(Message message)
    {
    }

    public int FailedToSend(Message message)
    {
        return message.SentAttempts;
    }

    public void SuccessfullySent(IEnumerable<Message> messages)
    {
    }

    public Message GetMessage(string queueName, MessageId messageId)
    {
        return null;
    }

    public string[] GetAllQueues()
    {
        return _queues.ToArray();
    }

    public void ClearAllStorage()
    {
    }
}