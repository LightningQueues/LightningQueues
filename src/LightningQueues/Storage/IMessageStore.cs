using System;
using System.Collections.Generic;

namespace LightningQueues.Storage;

public interface IMessageStore : IDisposable
{
    ITransaction BeginTransaction();
    void CreateQueue(string queueName);
    void StoreIncoming(params IEnumerable<Message> messages);
    void StoreIncoming(ITransaction transaction, params IEnumerable<Message> messages);
    void DeleteIncoming(params IEnumerable<Message> messages);
    IEnumerable<Message> PersistedIncoming(string queueName);
    IEnumerable<Message> PersistedOutgoing();
    void MoveToQueue(ITransaction transaction, string queueName, Message message);
    void SuccessfullyReceived(ITransaction transaction, Message message);
    void StoreOutgoing(ITransaction tx, Message message);
    void StoreOutgoing(params IEnumerable<Message> messages);
    int FailedToSend(Message message);
    void SuccessfullySent(params IEnumerable<Message> messages);
    Message GetMessage(string queueName, MessageId messageId);
    string[] GetAllQueues();
    void ClearAllStorage();
}