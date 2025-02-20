using System;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction : ITransaction
{
    private readonly ReaderWriterLockSlim _writeLock;

    public LmdbTransaction(LightningTransaction tx, ReaderWriterLockSlim writeLock)
    {
        _writeLock = writeLock;
        Transaction = tx;
    }
    
    public LightningTransaction Transaction { get; }

    void ITransaction.Commit()
    {
        if (!Transaction.Environment.IsOpened)
            return;
        Transaction.Commit().ThrowOnError();
    }

    public void Dispose()
    {
        try
        {
            if (!Transaction.Environment.IsOpened)
                return;
            Transaction?.Dispose();
        }
        finally
        {
            _writeLock.ExitWriteLock();
        }
    }
}