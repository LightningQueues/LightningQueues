using System;
using System.Reactive.Concurrency;
using System.Threading.Tasks;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction
    {
        private readonly LightningTransaction _transaction;
        private readonly IScheduler _scheduler;

        public LmdbTransaction(LightningTransaction transaction, IScheduler scheduler)
        {
            _transaction = transaction;
            _scheduler = scheduler;
            TransactionId = Guid.NewGuid();
        }

        public Guid TransactionId { get; }

        public Task Commit()
        {
            return TransactionAction(x => x.Commit());
        }

        public Task Rollback()
        {
            return TransactionAction(x => x.Dispose());
        }

        private Task TransactionAction(Action<LightningTransaction> action)
        {
            var tcs = new TaskCompletionSource<bool>();
            var catchAll = _scheduler.Catch<Exception>(ex =>
            {
                tcs.SetException(ex);
                return true;
            });
            catchAll.Schedule(() =>
            {
                action(_transaction);
                tcs.SetResult(true);
            });
            return tcs.Task;
        }
    }
}