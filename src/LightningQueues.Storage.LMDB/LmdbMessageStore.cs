using System;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
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

        public Task StoreMessages(IAsyncTransaction transaction, params Message[] messages)
        {
            return ExecuteScheduledAction(transaction, tx => StoreMessages(tx, messages));
        }

        private void StoreMessages(LightningTransaction tx, params Message[] messages)
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

        public Task MoveToQueue(IAsyncTransaction transaction, string queueName, Message message)
        {
            return ExecuteScheduledAction(transaction, tx => MoveToQueue(tx, queueName, message));
        }

        public Task FailedToSend(IAsyncTransaction transaction, params OutgoingMessage[] messages)
        {
            return ExecuteScheduledAction(transaction, tx =>
            {
                foreach (var message in messages)
                {
                    FailedToSend(tx, message);
                }
            });
        }

        private Task ExecuteScheduledAction(IAsyncTransaction transaction, Action<LightningTransaction> action)
        {
            //Ensures that all lmdb transactions that may coordinate across async Tasks are executed on the same thread.
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
                action(tx);
                tcs.SetResult(true);
            });
            return tcs.Task;
        }

        public Task SuccessfullySent(IAsyncTransaction transaction, params OutgoingMessage[] messages)
        {
            return ExecuteScheduledAction(transaction, tx => SuccessfullySent(tx, messages));
        }

        private void SuccessfullySent(LightningTransaction tx, params OutgoingMessage[] messages)
        {
            RemoveMessageFromStorage(tx, "outgoing", messages.Cast<Message>().ToArray());
        }

        public void StoreMessages(ITransaction transaction, params Message[] messages)
        {
            var tx = ((LmdbTransaction)transaction).Transaction;
            StoreMessages(tx, messages);
        }

        public IObservable<Message> PersistedMessages(string queueName)
        {
            return Observable.Create<Message>(x =>
            {
                try
                {
                    using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
                    using (var db = tx.OpenDatabase(queueName))
                    using (var cursor = tx.CreateCursor(db))
                    {
                        while (cursor.MoveNext())
                        {
                            var current = cursor.Current;
                            var message = new Message();
                            message.Id = MessageId.Parse(Encoding.UTF8.GetString(current.Key));
                            message.Data = current.Value;
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.Headers = current.Value.ToDictionary();
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.SentAt = DateTime.FromBinary(BitConverter.ToInt64(current.Value, 0));
                            message.Queue = queueName;
                            x.OnNext(message);
                        }
                    }
                    x.OnCompleted();
                }
                catch (Exception ex)
                {
                    x.OnError(ex);
                }
                return Disposable.Empty;
            });
        }

        public IObservable<OutgoingMessage> PersistedOutgoingMessages()
        {
            return Observable.Create<OutgoingMessage>(x =>
            {
                try
                {
                    using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
                    using (var db = tx.OpenDatabase("outgoing"))
                    using (var cursor = tx.CreateCursor(db))
                    {
                        while (cursor.MoveNext())
                        {
                            var current = cursor.Current;
                            var message = new OutgoingMessage();
                            var key = Encoding.UTF8.GetString(current.Key);
                            message.Id = MessageId.Parse(key);
                            message.Data = current.Value;
                            cursor.MoveNext();
                            cursor.MoveNext(); //move past attempts
                            current = cursor.Current;
                            var date = DateTime.FromBinary(BitConverter.ToInt64(current.Value, 0));
                            if (date != DateTime.MinValue)
                                message.DeliverBy = date;
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.Headers = current.Value.ToDictionary();
                            cursor.MoveNext();
                            current = cursor.Current;
                            var maxAttempts = BitConverter.ToInt32(current.Value, 0);
                            if (maxAttempts != 0)
                                message.MaxAttempts = maxAttempts;
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.Queue = Encoding.UTF8.GetString(current.Value);
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.SentAt = DateTime.FromBinary(BitConverter.ToInt64(current.Value, 0));
                            cursor.MoveNext();
                            current = cursor.Current;
                            message.Destination = new Uri(Encoding.UTF8.GetString(current.Value));
                            x.OnNext(message);
                        }
                    }
                    x.OnCompleted();
                }
                catch (Exception ex)
                {
                    x.OnError(ex);
                }
                return Disposable.Empty;
            });
        }

        public void MoveToQueue(ITransaction transaction, string queueName, Message message)
        {
            var tx = ((LmdbTransaction) transaction).Transaction;
            MoveToQueue(tx, queueName, message);
        }

        public void SuccessfullyReceived(ITransaction transaction, Message message)
        {
            var tx = ((LmdbTransaction) transaction).Transaction;
            SuccessfullyReceived(tx, message);
        }

        private void SuccessfullyReceived(LightningTransaction tx, Message message)
        {
            RemoveMessageFromStorage(tx, message.Queue, message);
        }

        private void RemoveMessageFromStorage(LightningTransaction tx, string queueName, params Message[] messages)
        {
            var db = tx.OpenDatabase(queueName);
            foreach (var message in messages)
            {
                using (var cursor = tx.CreateCursor(db))
                {
                    var idPrefix = Encoding.UTF8.GetBytes(message.Id.ToString());
                    while (cursor.MoveToFirstAfter(idPrefix))
                    {
                        var current = cursor.Current;
                        if (!current.Key.StartsWith(idPrefix))
                            break;

                        cursor.Delete();
                    }
                }
            }
        }

        public void StoreOutgoing(ITransaction transaction, OutgoingMessage message)
        {
            var tx = ((LmdbTransaction) transaction).Transaction;
            StoreOutgoing(tx, message);
        }

        private void StoreOutgoing(LightningTransaction tx, OutgoingMessage message)
        {
            var db = tx.OpenDatabase("outgoing");
            tx.Put(db, $"{message.Id}", message.Data);
            tx.Put(db, $"{message.Id}/attempts", BitConverter.GetBytes(0));
            tx.Put(db, $"{message.Id}/h", message.Headers.ToBytes());
            tx.Put(db, $"{message.Id}/q", Encoding.UTF8.GetBytes(message.Queue));
            tx.Put(db, $"{message.Id}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
            tx.Put(db, $"{message.Id}/uri", Encoding.UTF8.GetBytes(message.Destination.ToString()));
            //Possibly not insert here when null, but easier to deal with upstream for now
            var expire = message.DeliverBy ?? DateTime.MinValue;
            tx.Put(db, $"{message.Id}/expire", BitConverter.GetBytes(expire.ToBinary()));
            var maxAttempts = message.MaxAttempts ?? 0;
            tx.Put(db, $"{message.Id}/max", BitConverter.GetBytes(maxAttempts));
        }

        private void FailedToSend(LightningTransaction tx, OutgoingMessage message)
        {
            var key = $"{message.Id}/attempts";
            var db = tx.OpenDatabase("outgoing");
            var attemptBytes = tx.Get(db, key);
            var attempts = BitConverter.ToInt32(attemptBytes, 0);
            attempts += 1;
            attemptBytes = BitConverter.GetBytes(attempts);
            tx.Put(db, key, attemptBytes);
        }

        private void MoveToQueue(LightningTransaction tx, string queueName, Message message)
        {
            try
            {
                var original = tx.OpenDatabase(message.Queue);
                tx.Delete(original, message.Id.ToString());
                tx.Delete(original, $"{message.Id}/headers");
                tx.Delete(original, $"{message.Id}/sent");
                var newDb = tx.OpenDatabase(queueName);
                tx.Put(newDb, message.Id.ToString(), message.Data);
                tx.Put(newDb, $"{message.Id}/headers", message.Headers.ToBytes());
                tx.Put(newDb, $"{message.Id}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
            }
            catch (LightningException ex)
            {
                tx.Dispose();
                if (ex.StatusCode == -30798) //MDB_NOTFOUND
                    throw new QueueDoesNotExistException("Queue doesn't exist", ex);
                throw;
            }
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