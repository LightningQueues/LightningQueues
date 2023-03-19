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

    void ITransaction.Commit()
    {
        Transaction.Commit().ThrowOnError();
    }

    public void Dispose()
    {
        try
        {
            Transaction?.Dispose();
        }
        finally
        {
            _complete();
        }
    }
}