using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction
    {
        private readonly LightningTransaction _transaction;

        public LmdbTransaction(LightningTransaction transaction)
        {
            _transaction = transaction;
            TransactionId = Guid.NewGuid();
        }

        public Guid TransactionId { get; }

        public void Commit()
        {
            _transaction.Commit();
        }

        public void Rollback()
        {
            _transaction.Dispose();
        }
    }
}