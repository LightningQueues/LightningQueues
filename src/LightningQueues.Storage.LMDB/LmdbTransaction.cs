using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class LmdbTransaction : ITransaction
{
    public LmdbTransaction(LightningEnvironment env)
    {
        Transaction = env.BeginTransaction();
    }

    public LightningTransaction Transaction { get; }

    void ITransaction.Rollback()
    {
        Transaction.Dispose();
    }

    void ITransaction.Commit()
    {
        using(Transaction)
            Transaction.Commit().ThrowOnError();
    }
}