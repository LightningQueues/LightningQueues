using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using LightningDB;
using LightningQueues.Serialization;

namespace LightningQueues.Storage.LMDB;

public class LmdbMessageStore : IMessageStore
{
    private const string OutgoingQueue = "outgoing";
    private readonly ReaderWriterLockSlim _lock;
    private readonly LightningEnvironment _environment;
    private readonly IMessageSerializer _serializer;

    public LmdbMessageStore(string path, EnvironmentConfiguration config, IMessageSerializer serializer) : this(new LightningEnvironment(path, config), serializer)
    {
    }

    public LmdbMessageStore(string path, IMessageSerializer serializer) : this(path, new EnvironmentConfiguration {MapSize = 1024 * 1024 * 100, MaxDatabases = 5}, serializer)
    {
    }

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer)
    {
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _environment = environment;
        _serializer = serializer;
        if(!_environment.IsOpened)
            _environment.Open(EnvironmentOpenFlags.NoLock);
        CreateQueue(OutgoingQueue);
    }

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            StoreIncoming(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreIncoming(ITransaction transaction, params IEnumerable<Message> messages)
    {
        var tx = ((LmdbTransaction) transaction).Transaction;
        StoreIncoming(tx, messages);
    }

    private void StoreIncoming(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
        {
            var queueName = messagesByQueue.Key;
            var db = OpenDatabase(queueName);
            foreach (var message in messagesByQueue)
            {
                StoreIncomingMessage(tx, db, message);
            }
        }
    }

    private void StoreIncomingMessage(LightningTransaction tx, LightningDatabase db, Message message)
    {
        try
        {
            Span<byte> id = stackalloc byte[16];
            message.Id.MessageIdentifier.TryWriteBytes(id);
            ThrowIfError(tx.Put(db, id, _serializer.AsSpan(message)));
        }
        catch (StorageException ex)
        {
            if (ex.ResultCode == MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(message.Queue, ex);
            throw;
        }
    }

    public void DeleteIncoming(IEnumerable<Message> messages)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            foreach (var grouping in messages.GroupBy(x => x.Queue))
            {
                RemoveMessagesFromStorage(tx, grouping.Key, grouping);
            }

            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public ITransaction BeginTransaction()
    {
        _lock.EnterWriteLock();
        return new LmdbTransaction(_environment.BeginTransaction(), _lock);
    }

    public int FailedToSend(Message message)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            var result = FailedToSend(tx, message);
            ThrowIfError(tx.Commit());
            return result;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            SuccessfullySent(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Message GetMessage(string queueName, MessageId messageId)
    {
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        try
        {
            _lock.EnterReadLock();
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            var db = OpenDatabase(queueName);
            var result = tx.Get(db, id);
            ThrowIfReadError(result.resultCode);
            if (result.resultCode == MDBResultCode.NotFound)
                return null;
            var messageBuffer = result.value.AsSpan();
            return _serializer.ToMessage(messageBuffer);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public string[] GetAllQueues()
    {
        return GetAllQueuesImpl().Where(x => OutgoingQueue != x).ToArray();
    }

    public void ClearAllStorage()
    {
        var databases = GetAllQueuesImpl().ToArray();
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            foreach (var databaseName in databases)
            {
                var db = OpenDatabase(databaseName);
                ThrowIfError(tx.TruncateDatabase(db));
            }

            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private IEnumerable<string> GetAllQueuesImpl()
    {
        try
        {
            _lock.EnterReadLock();
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = tx.OpenDatabase();
            using var cursor = tx.CreateCursor(db);
            foreach (var (key, _) in cursor.AsEnumerable())
            {
                yield return Encoding.UTF8.GetString(key.CopyToNewArray());
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void SuccessfullySent(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        RemoveMessagesFromStorage(tx, OutgoingQueue, messages);
    }
        
    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        try
        {
            _lock.EnterReadLock();
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            var db = OpenDatabase(queueName);
            using var cursor = tx.CreateCursor(db);
            foreach (var (_, value) in cursor.AsEnumerable())
            {
                var valueSpan = value.AsSpan();
                var msg = _serializer.ToMessage(valueSpan);
                yield return msg;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        try
        {
            _lock.EnterReadLock();
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            var db = OpenDatabase(OutgoingQueue);
            using var cursor = tx.CreateCursor(db);
            foreach (var (_, value) in cursor.AsEnumerable())
            {
                var valueSpan = value.AsSpan();
                var msg = _serializer.ToMessage(valueSpan);
                yield return msg;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void MoveToQueue(ITransaction transaction, string queueName, Message message)
    {
        var tx = ((LmdbTransaction)transaction).Transaction;
        MoveToQueue(tx, queueName, message);
    }

    public void SuccessfullyReceived(ITransaction transaction, Message message)
    {
        var tx = ((LmdbTransaction) transaction).Transaction;
        SuccessfullyReceived(tx, message);
    }

    private void SuccessfullyReceived(LightningTransaction tx, Message message)
    {
        var db = OpenDatabase(message.Queue);
        RemoveMessageFromStorage(tx, db, message);
    }

    private void RemoveMessagesFromStorage<TMessage>(LightningTransaction tx, string queueName, params IEnumerable<TMessage> messages)
        where TMessage : Message
    {
        var db = OpenDatabase(queueName);
        foreach (var message in messages)
        {
            RemoveMessageFromStorage(tx, db, message);
        }
    }

    private static void RemoveMessageFromStorage<TMessage>(LightningTransaction tx, LightningDatabase db, TMessage message)
        where TMessage : Message
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        ThrowIfError(tx.Delete(db, id));
    }

    public void StoreOutgoing(ITransaction transaction, Message message)
    {
        var tx = ((LmdbTransaction) transaction).Transaction;
        StoreOutgoing(tx, message);
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            using var enumerator = messages.GetEnumerator();
            while (enumerator.MoveNext())
            {
                StoreOutgoing(tx, enumerator.Current);
            }
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void StoreOutgoing(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = OpenDatabase(OutgoingQueue);
        ThrowIfError(tx.Put(db, id, _serializer.AsSpan(message)));
    }

    private static void ThrowIfError(MDBResultCode resultCode)
    {
        if (resultCode != MDBResultCode.Success)
            throw new StorageException("Error with LightningDB operation", resultCode);
    }

    private static void ThrowIfReadError(MDBResultCode resultCode)
    {
        if (resultCode != MDBResultCode.Success && resultCode != MDBResultCode.NotFound)
            throw new StorageException("Error with LightningDB read operation", resultCode);
    }

    private int FailedToSend(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = OpenDatabase(OutgoingQueue);
        var value = tx.Get(db, id);
        if (value.resultCode == MDBResultCode.NotFound)
            return int.MaxValue;
        var valueBuffer = value.value.AsSpan();
        var msg = _serializer.ToMessage(valueBuffer);
        var attempts = message.SentAttempts;
        if (attempts >= message.MaxAttempts)
        {
            RemoveMessageFromStorage(tx, db, msg);
        }
        else if (msg.DeliverBy.HasValue)
        {
            var expire = msg.DeliverBy.Value;
            if (expire != DateTime.MinValue && DateTime.Now >= expire)
            {
                RemoveMessageFromStorage(tx, db, msg);
            }
        }
        else
        {
            ThrowIfError(tx.Put(db, id, _serializer.AsSpan(msg)));
        }
        return attempts;
    }

    private void MoveToQueue(LightningTransaction tx, string queueName, Message message)
    {
        try
        {
            Span<byte> id = stackalloc byte[16];
            message.Id.MessageIdentifier.TryWriteBytes(id);
            var original = OpenDatabase(message.Queue);
            var newDb = OpenDatabase(queueName);
            ThrowIfError(tx.Delete(original, id));
            ThrowIfError(tx.Put(newDb, id, _serializer.AsSpan(message)));
        }
        catch (LightningException ex)
        {
            tx.Dispose();
            if (ex.StatusCode == (int)MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(queueName, ex);
            throw;
        }
    }

    public void CreateQueue(string queueName)
    {
        try
        {
            _lock.EnterWriteLock();
            using var tx = _environment.BeginTransaction();
            var db = tx.OpenDatabase(queueName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            _databaseCache[queueName] = db;
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private readonly ConcurrentDictionary<string, LightningDatabase> _databaseCache = new();
    private LightningDatabase OpenDatabase(string database)
    {
        if (_databaseCache.TryGetValue(database, out var value))
            return value;
        throw new QueueDoesNotExistException(database);
    }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~LmdbMessageStore()
    {
        Dispose(false);
    }

    private void Dispose(bool disposing)
    {
        if (disposing)
        {
            foreach (var database in _databaseCache)
            {
                database.Value.Dispose();
            }
        }
        _environment.Dispose();
    }
}