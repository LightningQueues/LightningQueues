using System;
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
    private readonly Lock _lock;
    private readonly LightningEnvironment _environment;
    private readonly IMessageSerializer _serializer;
    private bool _disposed;

    public LmdbMessageStore(string path, EnvironmentConfiguration config, IMessageSerializer serializer) : this(new LightningEnvironment(path, config), serializer)
    {
    }

    public LmdbMessageStore(string path, IMessageSerializer serializer) : this(path, new EnvironmentConfiguration {MapSize = 1024 * 1024 * 100, MaxDatabases = 5}, serializer)
    {
    }

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer)
    {
        _lock = new Lock();
        _environment = environment;
        _serializer = serializer;
        if(!_environment.IsOpened)
            _environment.Open(EnvironmentOpenFlags.NoLock);
        CreateQueue(OutgoingQueue);
    }
    
    public string Path => _environment.Path;

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            StoreIncoming(tx, messages);
            ThrowIfError(tx.Commit());
        }
    }

    public void StoreIncoming(LmdbTransaction transaction, params IEnumerable<Message> messages)
    {
        var tx = transaction.Transaction;
        StoreIncoming(tx, messages);
    }

    private void StoreIncoming(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
        {
            var queueName = messagesByQueue.Key;
            using var db = OpenQueueDatabase(tx, queueName);
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
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            foreach (var grouping in messages.GroupBy(x => x.Queue))
            {
                RemoveMessagesFromStorage(tx, grouping.Key, grouping);
            }
            ThrowIfError(tx.Commit());
        }
    }

    private LightningDatabase OpenQueueDatabase(LightningTransaction tx, string queueName)
    {
        try
        {
            return tx.OpenDatabase(queueName);
        }
        catch (LightningException ex)
        {
            if (ex.StatusCode == (int)MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(queueName, ex);
            throw;
        }
    }

    public LmdbTransaction BeginTransaction()
    {
        var scope = _lock.EnterScope();
        return new LmdbTransaction(_environment.BeginTransaction(), scope);
    }

    public int FailedToSend(Message message)
    {
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            var result = FailedToSend(tx, message);
            ThrowIfError(tx.Commit());
            return result;
        }
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            SuccessfullySent(tx, messages);
            ThrowIfError(tx.Commit());
        }
    }

    public Message GetMessage(string queueName, MessageId messageId)
    {
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = OpenQueueDatabase(tx, queueName);
            var result = tx.Get(db, id);
            ThrowIfReadError(result.resultCode);
            if (result.resultCode == MDBResultCode.NotFound)
                return null;
            var messageBuffer = result.value.AsSpan();
            return _serializer.ToMessage(messageBuffer);
        }
    }

    public string[] GetAllQueues()
    {
        return GetAllQueuesImpl().Where(x => OutgoingQueue != x).ToArray();
    }

    public void ClearAllStorage()
    {
        var databases = GetAllQueuesImpl().ToArray();
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            foreach (var queueName in databases)
            {
                using var db = OpenQueueDatabase(tx, queueName);
                ThrowIfError(tx.TruncateDatabase(db));
            }

            ThrowIfError(tx.Commit());
        }
    }

    private IEnumerable<string> GetAllQueuesImpl()
    {
        var list = new List<string>();
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = tx.OpenDatabase();
            using var cursor = tx.CreateCursor(db);
            foreach (var (key, _) in cursor.AsEnumerable())
            {
                list.Add(Encoding.UTF8.GetString(key.CopyToNewArray()));
            }
        }
        return list;
    }

    private void SuccessfullySent(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        RemoveMessagesFromStorage(tx, OutgoingQueue, messages);
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        var list = new List<Message>();
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = OpenQueueDatabase(tx, queueName);
            using var cursor = tx.CreateCursor(db);
            foreach (var (_, value) in cursor.AsEnumerable())
            {
                var valueSpan = value.AsSpan();
                var msg = _serializer.ToMessage(valueSpan);
                list.Add(msg);
            }
        }

        return list;
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        var list = new List<Message>();
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = OpenQueueDatabase(tx, OutgoingQueue);
            using var cursor = tx.CreateCursor(db);
            foreach (var (_, value) in cursor.AsEnumerable())
            {
                var valueSpan = value.AsSpan();
                var msg = _serializer.ToMessage(valueSpan);
                list.Add(msg);
            }
        }
        return list;
    }

    public void MoveToQueue(LmdbTransaction transaction, string queueName, Message message)
    {
        var tx = transaction.Transaction;
        MoveToQueue(tx, queueName, message);
    }

    public void SuccessfullyReceived(LmdbTransaction transaction, Message message)
    {
        var tx = transaction.Transaction;
        SuccessfullyReceived(tx, message);
    }

    private void SuccessfullyReceived(LightningTransaction tx, Message message)
    {
        using var db = OpenQueueDatabase(tx, message.Queue);
        RemoveMessageFromStorage(tx, db, message);
    }

    private void RemoveMessagesFromStorage<TMessage>(LightningTransaction tx, string queueName, params IEnumerable<TMessage> messages)
        where TMessage : Message
    {
        using var db = OpenQueueDatabase(tx, queueName);
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

    public void StoreOutgoing(LmdbTransaction transaction, Message message)
    {
        var tx = transaction.Transaction;
        StoreOutgoing(tx, message);
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            using var enumerator = messages.GetEnumerator();
            while (enumerator.MoveNext())
            {
                StoreOutgoing(tx, enumerator.Current);
            }

            tx.Commit();
        }
    }

    private void StoreOutgoing(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        using var db = OpenQueueDatabase(tx, OutgoingQueue);
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
        using var db = OpenQueueDatabase(tx, OutgoingQueue);
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
            using var original = OpenQueueDatabase(tx, message.Queue);
            using var newDb = OpenQueueDatabase(tx, queueName);
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
        lock (_lock)
        {
            using var tx = _environment.BeginTransaction();
            using var db = tx.OpenDatabase(queueName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            ThrowIfError(tx.Commit());
        }
    }
    
    

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        lock (_lock)
        {
            Dispose(true);
        }
    }

    ~LmdbMessageStore()
    {
        Dispose(false);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        _disposed = true;
        _environment.Dispose();
    }
}