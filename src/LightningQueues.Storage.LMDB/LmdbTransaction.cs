using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction : ITransaction
{
    public LmdbTransaction(LightningEnvironment env)
    {
        Transaction = env.BeginTransaction();
        TransactionId = Guid.NewGuid();
    }

    public LightningTransaction Transaction { get; }

    public Guid TransactionId { get; }

    void ITransaction.Rollback()
    {
        Transaction.Dispose();
    }

    void ITransaction.Commit()
    {
        using(Transaction)
            Transaction.Commit();
    }
}