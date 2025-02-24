using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction(LightningTransaction tx) : IDisposable
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
        using(Transaction)
        {
        }
    }
}