using System.Threading;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public ref struct LmdbTransaction
{
    private readonly Lock.Scope _scope;

    public LmdbTransaction(LightningTransaction tx, Lock.Scope scope)
    {
        Transaction = tx;
        _scope = scope;
    }
    
    public LightningTransaction Transaction { get; }

    public void Commit()
    {
        if (!Transaction.Environment.IsOpened)
            return;
        Transaction.Commit().ThrowOnError();
    }

    public void Dispose()
    {
        using (_scope)
        using(Transaction)
        {
        }
    }
}