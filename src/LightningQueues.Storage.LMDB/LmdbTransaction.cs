using System;
using System.Reactive.Concurrency;
using System.Threading.Tasks;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction, IAsyncTransaction
    {
        private readonly LightningTransaction _transaction;
        private readonly IScheduler _scheduler;

        private LmdbTransaction(LightningTransaction transaction, IScheduler scheduler)
        {
            _scheduler = scheduler;
            _transaction = transaction;
            TransactionId = Guid.NewGuid();
        }

        public LmdbTransaction(LightningEnvironment env)
        {
            _transaction = env.BeginTransaction();
        }

        public LightningTransaction Transaction => _transaction;
        public IScheduler Scheduler => _scheduler;

        public static async Task<IAsyncTransaction> CreateAsync(LightningEnvironment env, IScheduler scheduler)
        {
            var tcs = new TaskCompletionSource<LightningTransaction>();
            scheduler.Schedule(() =>
            {
                try
                {
                    var tx = env.BeginTransaction();
                    tcs.SetResult(tx);
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            });
            var transaction = await tcs.Task;
            return new LmdbTransaction(transaction, scheduler);
        }

        public Guid TransactionId { get; }

        Task IAsyncTransaction.Commit()
        {
            return AsyncTransactionAction(x => x.Commit());
        }

        Task IAsyncTransaction.Rollback()
        {
            return AsyncTransactionAction(x => x.Dispose());
        }

        void ITransaction.Rollback()
        {
            _transaction.Dispose();
        }

        void ITransaction.Commit()
        {
            using(_transaction)
                _transaction.Commit();
        }

        private Task AsyncTransactionAction(Action<LightningTransaction> action)
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