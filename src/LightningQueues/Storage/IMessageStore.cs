using System;
using System.Collections.Generic;

namespace LightningQueues.Storage;

public interface IMessageStore : IDisposable
{
    ITransaction BeginTransaction();
    void CreateQueue(string queueName);
    void StoreIncomingMessages(params Message[] messages);
    void StoreIncomingMessages(ITransaction transaction, params Message[] messages);
    void DeleteIncomingMessages(params Message[] messages);
    IEnumerable<Message> PersistedMessages(string queueName);
    IEnumerable<OutgoingMessage> PersistedOutgoingMessages();
    void MoveToQueue(ITransaction transaction, string queueName, Message message);
    void SuccessfullyReceived(ITransaction transaction, Message message);
    void StoreOutgoing(ITransaction tx, OutgoingMessage message);
    int FailedToSend(OutgoingMessage message);
    void SuccessfullySent(params OutgoingMessage[] messages);
    Message GetMessage(string queueName, MessageId messageId);
    string[] GetAllQueues();
    void ClearAllStorage();
}