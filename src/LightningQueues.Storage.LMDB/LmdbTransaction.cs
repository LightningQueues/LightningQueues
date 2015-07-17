using System;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbTransaction : ITransaction, IAsyncTransaction
    {
        private readonly LightningTransaction _transaction;
        private readonly EventLoopScheduler _scheduler;
        private readonly Action<LightningTransaction> _commit; 
        private readonly Action<LightningTransaction> _dispose; 

        private LmdbTransaction(LightningTransaction transaction, EventLoopScheduler scheduler)
        {
            _scheduler = scheduler;
            _transaction = transaction;
            TransactionId = Guid.NewGuid();
            _commit = tx =>
            {
                using(_scheduler)
                using(tx)
                    tx.Commit();
            };
            _dispose = tx =>
            {
                using(_scheduler)
                    tx.Dispose();
            };
        }

        public LmdbTransaction(LightningEnvironment env)
        {
            _transaction = env.BeginTransaction();
        }

        public LightningTransaction Transaction => _transaction;
        public IScheduler Scheduler => _scheduler;

        public static async Task<IAsyncTransaction> CreateAsync(LightningEnvironment env, EventLoopScheduler scheduler)
        {
            var tcs = new TaskCompletionSource<LightningTransaction>();
            scheduler.Schedule(tcs, (sch, completionSource) =>
            {
                try
                {
                    var tx = env.BeginTransaction();
                    completionSource.SetResult(tx);
                }
                catch (Exception ex)
                {
                    completionSource.SetException(ex);
                }
                return Disposable.Empty;
            });
            var transaction = await tcs.Task;
            return new LmdbTransaction(transaction, scheduler);
        }

        public Guid TransactionId { get; }

        Task IAsyncTransaction.Commit()
        {
            return AsyncTransactionAction(_commit);
        }

        Task IAsyncTransaction.Rollback()
        {
            return AsyncTransactionAction(_dispose);
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
            _scheduler.Schedule(tcs, (sch, completionSource) =>
            {
                try
                {
                    action(_transaction);
                    completionSource.SetResult(true);
                }
                catch (Exception ex)
                {
                    completionSource.SetException(ex);
                }
                return Disposable.Empty;
            });
            return tcs.Task;
        }
    }
}