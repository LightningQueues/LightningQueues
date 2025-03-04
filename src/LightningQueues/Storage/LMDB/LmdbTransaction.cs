using System;
using System.Threading;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction(LightningTransaction tx, ReaderWriterLockSlim transactionLock)
    : IDisposable
{
    public LightningTransaction Transaction { get; } = tx;

    public void Commit()
    {
        if (!Transaction.Environment.IsOpened)
            return;
        Transaction.Commit().ThrowOnError();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        try
        {
            Transaction.Dispose();
        }
        finally
        {
            transactionLock.ExitWriteLock();
        }
    }
}