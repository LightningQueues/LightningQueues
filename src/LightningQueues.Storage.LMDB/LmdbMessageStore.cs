using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbMessageStore : IMessageStore
    {
        private readonly LightningEnvironment _environment;
        private readonly string[] _outgoingKeys = { "{0}", "{0}/attempts", "{0}/h", "{0}/q", "{0}/sent", "{0}/uri", "{0}/expire", "{0}/max" };
        private readonly string[] _queueKeys = { "{0}", "{0}/headers", "{0}/sent" };
        private const string OutgoingQueue = "outgoing";

        public LmdbMessageStore(string path)
        {
            _environment = new LightningEnvironment(path)
            {
                MaxDatabases = 5,
                MapSize = 1024*1024*100
            };
            _environment.Open();
            CreateQueue(OutgoingQueue);
        }

        public LightningEnvironment Environment => _environment;

        public void StoreIncomingMessages(params Message[] messages)
        {
            using (var tx = _environment.BeginTransaction())
            {
                StoreIncomingMessages(tx, messages);
                tx.Commit();
            }
        }

        public void StoreIncomingMessages(ITransaction transaction, params Message[] messages)
        {
            var tx = ((LmdbTransaction) transaction).Transaction;
            StoreIncomingMessages(tx, messages);
        }

        private void StoreIncomingMessages(LightningTransaction tx, params Message[] messages)
        {
            try
            {

                foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
                {
                    var db = OpenDatabase(tx, messagesByQueue.Key);
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
                if (ex.StatusCode == LightningDB.Native.Lmdb.MDB_NOTFOUND)
                    throw new QueueDoesNotExistException("Queue doesn't exist", ex);
                throw;
            }
        }

        public void DeleteIncomingMessages(params Message[] messages)
        {
            using (var tx = _environment.BeginTransaction())
            {
                foreach (var grouping in messages.GroupBy(x => x.Queue))
                {
                    RemoveMessageFromStorage(tx, grouping.Key, _queueKeys, grouping.ToArray());
                }
                tx.Commit();
            }
        }

        public ITransaction BeginTransaction()
        {
            return new LmdbTransaction(_environment);
        }

        public int FailedToSend(OutgoingMessage message)
        {
            using (var tx = _environment.BeginTransaction())
            {
                var result = FailedToSend(tx, message);
                tx.Commit();
                return result;
            }
        }

        public void SuccessfullySent(params OutgoingMessage[] messages)
        {
            using (var tx = _environment.BeginTransaction())
            {
                SuccessfullySent(tx, messages);
                tx.Commit();
            }
        }

        public string[] GetAllQueues()
        {
            return GetAllQueuesImpl().Where(x => OutgoingQueue != x).ToArray();
        }

        public void ClearAllStorage()
        {
            var databases = GetAllQueuesImpl().ToArray();
            using (var tx = _environment.BeginTransaction())
            {
                foreach (var databaseName in databases)
                {
                    var db = OpenDatabase(tx, databaseName);
                    tx.TruncateDatabase(db);
                }
                tx.Commit();
            }
        }

        private IEnumerable<string> GetAllQueuesImpl()
        {
            using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
            using(var db = tx.OpenDatabase())
            using (var cursor = tx.CreateCursor(db))
            {
                foreach (var item in cursor)
                {
                    yield return Encoding.UTF8.GetString(item.Key);
                }
            }
        }

        private void SuccessfullySent(LightningTransaction tx, params OutgoingMessage[] messages)
        {
            RemoveMessageFromStorage(tx, "outgoing", _outgoingKeys, messages);
        }

        public IObservable<Message> PersistedMessages(string queueName)
        {
            return Observable.Create<Message>(x =>
            {
                try
                {
                    using (var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly))
                    {
                        var db = OpenDatabase(tx, queueName);
                        using (var cursor = tx.CreateCursor(db))
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
                    {
                        var db = OpenDatabase(tx, "outgoing");
                        using (var cursor = tx.CreateCursor(db))
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
            RemoveMessageFromStorage(tx, message.Queue, _queueKeys, message);
        }

        private void RemoveMessageFromStorage<T>(LightningTransaction tx, string queueName, string[] keys,
            params T[] messages) where T : Message
        {
            var db = OpenDatabase(tx, queueName);
            foreach (var message in messages)
            {
                foreach (var keyFormat in keys)
                {
                    var key = string.Format(keyFormat, message.Id);
                    tx.Delete(db, Encoding.UTF8.GetBytes(key));
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
            var db = OpenDatabase(tx, "outgoing");
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

        private int FailedToSend(LightningTransaction tx, OutgoingMessage message)
        {
            var key = $"{message.Id}/attempts";
            var db = OpenDatabase(tx, "outgoing");
            var attemptBytes = tx.Get(db, key);
            var attempts = BitConverter.ToInt32(attemptBytes, 0);
            attempts += 1;
            if (attempts >= message.MaxAttempts)
            {
                RemoveMessageFromStorage(tx, "outgoing", _outgoingKeys, message);
            }
            else
            {
                var expireBytes = tx.Get(db, $"{message.Id}/expire");
                var expire = DateTime.FromBinary(BitConverter.ToInt64(expireBytes, 0));
                if (expire != DateTime.MinValue && DateTime.Now >= expire)
                {
                    RemoveMessageFromStorage(tx, "outgoing", _outgoingKeys, message);
                }
                else
                {
                    attemptBytes = BitConverter.GetBytes(attempts);
                }
                tx.Put(db, key, attemptBytes);
            }
            return attempts;
        }

        private void MoveToQueue(LightningTransaction tx, string queueName, Message message)
        {
            try
            {
                var original = OpenDatabase(tx, message.Queue);
                tx.Delete(original, message.Id.ToString());
                tx.Delete(original, $"{message.Id}/headers");
                tx.Delete(original, $"{message.Id}/sent");
                var newDb = OpenDatabase(tx, queueName);
                tx.Put(newDb, message.Id.ToString(), message.Data);
                tx.Put(newDb, $"{message.Id}/headers", message.Headers.ToBytes());
                tx.Put(newDb, $"{message.Id}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
            }
            catch (LightningException ex)
            {
                tx.Dispose();
                if (ex.StatusCode == LightningDB.Native.Lmdb.MDB_NOTFOUND)
                    throw new QueueDoesNotExistException("Queue doesn't exist", ex);
                throw;
            }
        }

        public void CreateQueue(string queueName)
        {
            using (var tx = _environment.BeginTransaction())
            {
                var db = tx.OpenDatabase(queueName, new DatabaseConfiguration {Flags = DatabaseOpenFlags.Create});
                _databaseCache[queueName] = db;
                tx.Commit();
            }
        }

        private readonly ConcurrentDictionary<string, LightningDatabase> _databaseCache = new ConcurrentDictionary<string, LightningDatabase>();
        private LightningDatabase OpenDatabase(LightningTransaction transaction, string database)
        {
            if (_databaseCache.ContainsKey(database))
                return _databaseCache[database];
            var db = transaction.OpenDatabase(database);
            _databaseCache[database] = db;
            return db;
        }

        public void Dispose()
        {
            foreach (var database in _databaseCache)
            {
                database.Value.Dispose();
            }
            _environment.Dispose();
        }
    }
}