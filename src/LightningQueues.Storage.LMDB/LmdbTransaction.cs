using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction
    {
        private readonly LightningTransaction _transaction;

        private LmdbTransaction(LightningTransaction transaction)
        {
            _transaction = transaction;
            TransactionId = Guid.NewGuid();
        }

        public LmdbTransaction(LightningEnvironment env)
        {
            _transaction = env.BeginTransaction();
        }

        public LightningTransaction Transaction => _transaction;

        public Guid TransactionId { get; }

        void ITransaction.Rollback()
        {
            _transaction.Dispose();
        }

        void ITransaction.Commit()
        {
            using(_transaction)
                _transaction.Commit();
        }
    }
}