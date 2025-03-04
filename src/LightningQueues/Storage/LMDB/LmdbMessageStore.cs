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
    private readonly ReaderWriterLockSlim _lock;
    private readonly LightningEnvironment _environment;
    private readonly IMessageSerializer _serializer;
    private bool _disposed;

    public LmdbMessageStore(LightningEnvironment environment, IMessageSerializer serializer)
    {
        _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        _environment = environment;
        _serializer = serializer;
        if(!_environment.IsOpened)
            _environment.Open(EnvironmentOpenFlags.NoLock);
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
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
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

    public Message GetMessage(string queueName, MessageId messageId)
    {
        CheckDisposed();
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        
        _lock.EnterReadLock();
        try
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
                using var db = OpenQueueDatabase(tx, queueName);
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

    // Use a direct enumeration with reader locks, as they're more efficient for concurrent access
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
            // Load all messages at once with a single reader lock
            // This is more efficient than trying to yield inside a lock
            var messages = new List<Message>();
            
            _store._lock.EnterReadLock();
            try
            {
                using var tx = _store._environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
                using var db = _store.OpenQueueDatabase(tx, _queueName);
                using var cursor = tx.CreateCursor(db);
                
                foreach (var (_, value) in cursor.AsEnumerable())
                {
                    var valueSpan = value.AsSpan();
                    var msg = _store._serializer.ToMessage(valueSpan);
                    messages.Add(msg);
                }
            }
            finally
            {
                _store._lock.ExitReadLock();
            }
            
            // Return the enumeration of the loaded messages
            return messages.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
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
        CheckDisposed();
        var tx = transaction.Transaction;
        StoreOutgoing(tx, message);
    }

    public void StoreOutgoing(IEnumerable<Message> messages)
    {
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            foreach (var message in messages)
            {
                StoreOutgoing(tx, message);
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

    private void FailedToSend(LightningTransaction tx, Message message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        using var db = OpenQueueDatabase(tx, OutgoingQueue);
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
        CheckDisposed();
        
        _lock.EnterWriteLock();
        try
        {
            using var tx = _environment.BeginTransaction();
            using var db = tx.OpenDatabase(queueName, new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            ThrowIfError(tx.Commit());
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