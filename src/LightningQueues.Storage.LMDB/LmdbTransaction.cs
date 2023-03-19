using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction : ITransaction
{
    private readonly Action _complete;

    public LmdbTransaction(LightningTransaction tx, Action complete)
    {
        _complete = complete;
        Transaction = tx;
    }
    
    public LightningTransaction Transaction { get; }

    void ITransaction.Rollback()
    {
        try
        {
            Transaction.Dispose();
        }
        finally
        {
            _complete();
        }
    }

    void ITransaction.Commit()
    {
        try
        {
            using (Transaction)
                Transaction.Commit().ThrowOnError();
        }
        finally
        {
            _complete();
        }
    }
}