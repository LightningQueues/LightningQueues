using System;

namespace LightningQueues.Storage
{
    public interface IMessageStore : IDisposable
    {
        IAsyncMessageStore Async { get; }
        ITransaction BeginTransaction();
        void CreateQueue(string queueName);
        void StoreMessages(ITransaction transaction, params Message[] messages);
        IObservable<Message> PersistedMessages(string queueName);
        IObservable<OutgoingMessage> PersistedOutgoingMessages();
        void MoveToQueue(ITransaction transaction, string queueName, Message message);
        void SuccessfullyReceived(Message message);
        void SendMessage(Uri destination, Message message);
        void StoreOutgoing(ITransaction tx, OutgoingMessage message);
    }
}
