using System;
using System.Collections.Generic;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction
    {
        private readonly LightningTransaction _transaction;
        private readonly List<LightningDatabase> _databases;

        public LmdbTransaction(LightningTransaction transaction, List<LightningDatabase> databases)
        {
            _transaction = transaction;
            _databases = databases;
            TransactionId = Guid.NewGuid();
        }

        public Guid TransactionId { get; }

        public void Commit()
        {
            _transaction.Commit();
            _databases.CloseAll();
        }

        public void Rollback()
        {
            _transaction.Abort();
            _transaction.Dispose();
            _databases.CloseAll();
        }
    }
}