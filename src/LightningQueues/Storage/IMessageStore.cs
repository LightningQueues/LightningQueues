using System;
using System.Collections.Generic;

namespace LightningQueues.Storage;

public interface IMessageStore : IDisposable
{
    ITransaction BeginTransaction();
    void CreateQueue(string queueName);
    void StoreIncomingMessage(Message message);
    void StoreIncomingMessages(IEnumerable<Message> messages);
    void StoreIncomingMessage(ITransaction transaction, Message message);
    void StoreIncomingMessages(ITransaction transaction, IEnumerable<Message> messages);
    void DeleteIncomingMessages(IEnumerable<Message> messages);
    IEnumerable<Message> PersistedMessages(string queueName);
    IEnumerable<Message> PersistedOutgoingMessages();
    void MoveToQueue(ITransaction transaction, string queueName, Message message);
    void SuccessfullyReceived(ITransaction transaction, Message message);
    void StoreOutgoing(ITransaction tx, Message message);
    void StoreOutgoing(IEnumerable<Message> messages);
    void StoreOutgoing(Message message);
    int FailedToSend(Message message);
    void SuccessfullySent(IEnumerable<Message> messages);
    Message GetMessage(string queueName, MessageId messageId);
    string[] GetAllQueues();
    void ClearAllStorage();
}