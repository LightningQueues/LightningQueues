using System;

namespace LightningQueues.Storage
{
    public interface IMessageStore : IDisposable
    {
        IAsyncMessageStore Async { get; }
        ITransaction BeginTransaction();
        void CreateQueue(string queueName);
        void StoreMessages(ITransaction transaction, params IncomingMessage[] messages);
        IObservable<IncomingMessage> PersistedMessages(string queueName);
        void MoveToQueue(ITransaction transaction, string queueName, IncomingMessage message);
    }
}
