using System;

namespace LightningQueues.Storage
{
    public interface ITransaction : IStorage
    {
        Guid TransactionId { get; }
        void Commit();
        void Rollback();
    }
}
