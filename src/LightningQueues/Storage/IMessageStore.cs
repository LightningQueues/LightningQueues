using System;
using System.Collections.Generic;
using LightningDB;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Storage;

public interface IMessageStore : IDisposable
{
    LmdbTransaction BeginTransaction();
    void CreateQueue(string queueName);
    void StoreIncoming(params IEnumerable<Message> messages);
    void StoreIncoming(LmdbTransaction transaction, params IEnumerable<Message> messages);
    void DeleteIncoming(params IEnumerable<Message> messages);
    IEnumerable<Message> PersistedIncoming(string queueName);
    IEnumerable<Message> PersistedOutgoing();
    void MoveToQueue(LmdbTransaction transaction, string queueName, Message message);
    void SuccessfullyReceived(LmdbTransaction transaction, Message message);
    void StoreOutgoing(LmdbTransaction tx, Message message);
    void StoreOutgoing(params IEnumerable<Message> messages);
    int FailedToSend(Message message);
    void SuccessfullySent(params IEnumerable<Message> messages);
    Message GetMessage(string queueName, MessageId messageId);
    string[] GetAllQueues();
    void ClearAllStorage();
}