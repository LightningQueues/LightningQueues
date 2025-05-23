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
    private readonly ConcurrentDictionary<string, LightningDatabase> _cachedDatabases;
    private bool _disposed;

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer)
    {
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _environment = environment;
        _serializer = serializer;
        _cachedDatabases = new ConcurrentDictionary<string, LightningDatabase>();
        if(!_environment.IsOpened)
            _environment.Open(EnvironmentOpenFlags.NoLock | EnvironmentOpenFlags.MapAsync);
        CreateQueue(OutgoingQueue);
    }
    
    public string Path => _environment.Path;

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            StoreIncoming(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }
    
    private void CheckDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(LmdbMessageStore), "Cannot perform operation on a disposed message store");
    }

    public void StoreIncoming(LmdbTransaction transaction, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        var tx = transaction.Transaction;
        StoreIncoming(tx, messages);
    }

    private void StoreIncoming(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        foreach (var messagesByQueue in messages.GroupBy(x => x.QueueString))
        {
            var queueName = messagesByQueue.Key;
            var db = GetCachedDatabase(queueName);
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
                throw new QueueDoesNotExistException(message.QueueString, ex);
            throw;
        }
    }

    public void DeleteIncoming(IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var grouping in messages.GroupBy(x => x.QueueString))
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

    private LightningDatabase GetCachedDatabase(string queueName)
    {
        if (_cachedDatabases.TryGetValue(queueName, out var cachedDb))
            return cachedDb;
        
        throw new QueueDoesNotExistException(queueName);
    }

    public LmdbTransaction BeginTransaction()
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            // Note: When returning a transaction, caller is responsible for commit/rollback
            return new LmdbTransaction(_environment.BeginTransaction(), _lock);
        }
        catch
        {
            _lock.ExitWriteLock();
            throw;
        }
    }

    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var message in messages)
            {
                FailedToSend(tx, message);
            }
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            SuccessfullySent(tx, messages);
            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Message? GetMessage(string queueName, MessageId messageId)
    {
        CheckDisposed();
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        
        _lock.EnterReadLock();
        try
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            var db = GetCachedDatabase(queueName);
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
        CheckDisposed();
        // Filter out the outgoing queue
        return GetAllQueuesImpl()
            .Where(queueName => queueName != OutgoingQueue)
            .ToArray();
    }

    public void ClearAllStorage()
    {
        CheckDisposed();
        var queueNames = GetAllQueuesImpl();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var queueName in queueNames)
            {
                var db = GetCachedDatabase(queueName);
                ThrowIfError(tx.TruncateDatabase(db));
            }

            ThrowIfError(tx.Commit());
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private List<string> GetAllQueuesImpl()
    {
        // Pre-allocate with a reasonable size to avoid resizing
        var list = new List<string>(10);
        
        _lock.EnterReadLock();
        try
        {
            using var tx = _environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
            using var db = tx.OpenDatabase();
            using var cursor = tx.CreateCursor(db);
            
            // LightningDB specific handling
            foreach (var (key, _) in cursor.AsEnumerable())
            {
                var keyBytes = key.AsSpan();
                var queueName = Encoding.UTF8.GetString(keyBytes);
                list.Add(queueName);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
        return list;
    }

    private void SuccessfullySent(LightningTransaction tx, params IEnumerable<Message> messages)
    {
        RemoveMessagesFromStorage(tx, OutgoingQueue, messages);
    }

    // Streaming enumeration that yields messages one at a time for better memory efficiency
    private class MessageEnumerable : IEnumerable<Message>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;

        public MessageEnumerable(LmdbMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
        }

        public IEnumerator<Message> GetEnumerator()
        {
            return new MessageEnumerator(_store, _queueName);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private class MessageEnumerator : IEnumerator<Message>
    {
        private readonly LmdbMessageStore _store;
        private readonly string _queueName;
        private LightningTransaction _transaction;
        private LightningDatabase _database;
        private LightningCursor _cursor;
        private IEnumerator<(LightningDB.MDBValue key, LightningDB.MDBValue value)> _cursorEnumerator;
        private bool _disposed;

        public MessageEnumerator(LmdbMessageStore store, string queueName)
        {
            _store = store;
            _queueName = queueName;
            Initialize();
        }

        private void Initialize()
        {
            _store._lock.EnterReadLock();
            try
            {
                _transaction = _store._environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
                _database = _store.GetCachedDatabase(_queueName);
                _cursor = _transaction.CreateCursor(_database);
                _cursorEnumerator = _cursor.AsEnumerable().GetEnumerator();
            }
            catch
            {
                Cleanup();
                throw;
            }
        }

        public Message Current { get; private set; }

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_disposed || _cursorEnumerator == null)
                return false;

            try
            {
                if (_cursorEnumerator.MoveNext())
                {
                    var (_, value) = _cursorEnumerator.Current;
                    var valueSpan = value.AsSpan();
                    Current = _store._serializer.ToMessage(valueSpan);
                    return true;
                }
            }
            catch
            {
                // If there's an error, we should stop enumeration
                return false;
            }

            return false;
        }

        public void Reset()
        {
            Cleanup();
            Initialize();
        }

        private void Cleanup()
        {
            try
            {
                _cursorEnumerator?.Dispose();
                _cursor?.Dispose();
                _transaction?.Dispose();
            }
            finally
            {
                _store._lock.ExitReadLock();
                _cursorEnumerator = null;
                _cursor = null;
                _database = null;
                _transaction = null;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Cleanup();
                _disposed = true;
            }
        }
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        CheckDisposed();
        return new MessageEnumerable(this, queueName);
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        CheckDisposed();
        return new MessageEnumerable(this, OutgoingQueue);
    }

    public void MoveToQueue(LmdbTransaction transaction, string queueName, Message message)
    {
        CheckDisposed();
        var tx = transaction.Transaction;
        MoveToQueue(tx, queueName, message);
    }

    public void SuccessfullyReceived(LmdbTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = transaction.Transaction;
        SuccessfullyReceived(tx, message);
    }

    private void SuccessfullyReceived(LightningTransaction tx, Message message)
    {
        var db = GetCachedDatabase(message.QueueString);
        RemoveMessageFromStorage(tx, db, message);
    }

    private void RemoveMessagesFromStorage(LightningTransaction tx, string queueName, params IEnumerable<Message> messages)
    {
        var db = GetCachedDatabase(queueName);
        foreach (var message in messages)
        {
            RemoveMessageFromStorage(tx, db, message);
        }
    }

    private static void RemoveMessageFromStorage(LightningTransaction tx, LightningDatabase db, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        ThrowIfError(tx.Delete(db, id));
    }

    public void StoreOutgoing(LmdbTransaction transaction, Message message)
    {
        CheckDisposed();
        var tx = transaction.Transaction;
        var db = GetCachedDatabase(OutgoingQueue);
        StoreOutgoing(tx, db, message);
    }

    public void StoreOutgoing(Message message)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            StoreOutgoing(tx, db, message);
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreOutgoing(tx, db, message);
            }
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void StoreOutgoing(ReadOnlySpan<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = GetCachedDatabase(OutgoingQueue);
            foreach (var message in messages)
            {
                StoreOutgoing(tx, db, message);
            }
            tx.Commit();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void StoreOutgoing(LightningTransaction tx, LightningDatabase db, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
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

    private void FailedToSend(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = GetCachedDatabase(OutgoingQueue);
        var value = tx.Get(db, id);
        if (value.resultCode == MDBResultCode.NotFound)
            return;
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
    }

    private void MoveToQueue(LightningTransaction tx, string queueName, Message message)
    {
        try
        {
            Span<byte> id = stackalloc byte[16];
            message.Id.MessageIdentifier.TryWriteBytes(id);
            var original = GetCachedDatabase(message.QueueString);
            var newDb = GetCachedDatabase(queueName);
            ThrowIfError(tx.Delete(original, id));
            
            // Create updated message with new queue name for storage
            var updatedMessage = new Message(
                message.Id,
                message.Data,
                queueName.AsMemory(),
                message.SentAt,
                message.SubQueue,
                message.DestinationUri,
                message.DeliverBy,
                message.MaxAttempts,
                message.Headers
            );
            ThrowIfError(tx.Put(newDb, id, _serializer.AsSpan(updatedMessage)));
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
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            var db = tx.OpenDatabase(queueName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            ThrowIfError(tx.Commit());
            
            // Cache the database handle
            _cachedDatabases.TryAdd(queueName, db);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }



    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Dispose(true);
    }

    ~LmdbMessageStore()
    {
        Dispose(false);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
            return;
            
        // First mark as disposed to prevent new operations
        _disposed = true;
        
        if (disposing)
        {
            // Use write lock to ensure we don't dispose while another operation is in progress
            _lock.EnterWriteLock();
            try
            {
                try
                {
                    _cachedDatabases.Clear();
                    
                    //Flush due to async setup for more optimal performance
                    _environment.Flush(true);
                    // Dispose the environment in a controlled manner
                    _environment.Dispose();
                }
                catch (Exception)
                {
                    // Swallow exceptions during disposal to prevent disruption
                    // We're already tearing down, so not much we can do about failures
                }
            }
            finally
            {
                _lock.ExitWriteLock();
                
                // Dispose the lock itself
                _lock.Dispose();
            }
        }
    }
}