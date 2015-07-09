using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading.Tasks;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbMessageStore : IMessageStore, IAsyncMessageStore
    {
        private readonly LightningEnvironment _environment;

        public LmdbMessageStore(string path)
        {
            _environment = new LightningEnvironment(path) {MaxDatabases = 5};
            _environment.Open();
        }

        public LightningEnvironment Environment => _environment;

        public Task StoreMessages(IAsyncTransaction transaction, params IncomingMessage[] messages)
        {
            var lmdbTransaction = (LmdbTransaction)transaction;
            var tcs = new TaskCompletionSource<bool>();
            var scheduler = lmdbTransaction.Scheduler;
            var tx = lmdbTransaction.Transaction;
            var catchAll = scheduler.Catch<Exception>(ex =>
            {
                tcs.SetException(ex);
                return true;
            });
            catchAll.Schedule(() =>
            {
                StoreMessages(tx, messages);
                tcs.SetResult(true);
            });
            return tcs.Task;
        }

        private void StoreMessages(LightningTransaction tx, params IncomingMessage[] messages)
        {
            try
            {
                foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
                {
                    var db = tx.OpenDatabase(messagesByQueue.Key);
                    foreach (var message in messagesByQueue)
                    {
                        tx.Put(db, message.Id.ToString(), message.Data);
                        tx.Put(db, $"{message.Id}/headers", message.Headers.ToBytes());
                        tx.Put(db, $"{message.Id}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
                    }
                }
            }
            catch (LightningException ex)
            {
                tx.Dispose();
                if (ex.StatusCode == -30798) //MDB_NOTFOUND
                    throw new QueueDoesNotExistException("Queue doesn't exist", ex);
                throw;
            }
        }

        public IAsyncMessageStore Async => this;

        public ITransaction BeginTransaction()
        {
            return new LmdbTransaction(_environment);
        }

        public Task MoveToQueue(IAsyncTransaction transaction, string queueName, IncomingMessage message)
        {
            return Task.FromResult(0);
        }

        public void StoreMessages(ITransaction transaction, params IncomingMessage[] messages)
        {
            var tx = ((LmdbTransaction)transaction).Transaction;
            StoreMessages(tx, messages);
        }

        public IObservable<IncomingMessage> PersistedMessages(string queueName)
        {
            return Observable.Never<IncomingMessage>();
        }

        public void MoveToQueue(ITransaction transaction, string queueName, IncomingMessage message)
        {
        }

        public void CreateQueue(string queueName)
        {
            using (var tx = _environment.BeginTransaction())
            {
                using (tx.OpenDatabase(queueName, new DatabaseConfiguration {Flags = DatabaseOpenFlags.Create}))
                {
                    tx.Commit();
                }
            }
        }

        public void Dispose()
        {
            _environment.Dispose();
        }

        async Task<IAsyncTransaction> IAsyncMessageStore.BeginTransaction()
        {
            return await LmdbTransaction.CreateAsync(_environment, new EventLoopScheduler());
        }
    }
}