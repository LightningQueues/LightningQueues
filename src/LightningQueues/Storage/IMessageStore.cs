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
    IEnumerable<OutgoingMessage> PersistedOutgoingMessages();
    void MoveToQueue(ITransaction transaction, string queueName, Message message);
    void SuccessfullyReceived(ITransaction transaction, Message message);
    void StoreOutgoing(ITransaction tx, OutgoingMessage message);
    void StoreOutgoing(IEnumerable<OutgoingMessage> messages);
    void StoreOutgoing(OutgoingMessage message);
    int FailedToSend(OutgoingMessage message);
    void SuccessfullySent(IEnumerable<OutgoingMessage> messages);
    Message GetMessage(string queueName, MessageId messageId);
    string[] GetAllQueues();
    void ClearAllStorage();
}