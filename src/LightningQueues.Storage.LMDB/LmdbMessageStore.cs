using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LightningDB;
using LightningQueues.Serialization;
using DotNext.IO;

namespace LightningQueues.Storage.LMDB;

public class LmdbMessageStore : IMessageStore
{
    private const string OutgoingQueue = "outgoing";

    public LmdbMessageStore(string path, EnvironmentConfiguration config) : this(new LightningEnvironment(path, config))
    {
    }

    public LmdbMessageStore(string path) : this(path, new EnvironmentConfiguration {MapSize = 1024 * 1024 * 100, MaxDatabases = 5})
    {
    }

    public LmdbMessageStore(LightningEnvironment environment)
    {
        Environment = environment;
        Environment.Open();
        CreateQueue(OutgoingQueue);
    }

    public LightningEnvironment Environment { get; }

    public void StoreIncomingMessages(params Message[] messages)
    {
        using var tx = Environment.BeginTransaction();
        StoreIncomingMessages(tx, messages);
        tx.Commit().ThrowOnError();
    }

    public void StoreIncomingMessages(ITransaction transaction, params Message[] messages)
    {
        var tx = ((LmdbTransaction) transaction).Transaction;
        StoreIncomingMessages(tx, messages);
    }

    private void StoreIncomingMessages(LightningTransaction tx, params Message[] messages)
    {
        string queueName = null;
        try
        {
            Span<byte> id = stackalloc byte[16];
            foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
            {
                queueName = messagesByQueue.Key;
                var db = OpenDatabase(queueName);
                foreach (var message in messagesByQueue)
                {
                    message.Id.MessageIdentifier.TryWriteBytes(id);
                    tx.Put(db, id, message.AsReadOnlyMemory().Span).ThrowOnError();
                }
            }
        }
        catch (LightningException ex)
        {
            if (ex.StatusCode == (int)MDBResultCode.NotFound)
                throw new QueueDoesNotExistException(queueName, ex);
            throw;
        }
    }

    public void DeleteIncomingMessages(params Message[] messages)
    {
        using var tx = Environment.BeginTransaction();
        foreach (var grouping in messages.GroupBy(x => x.Queue))
        {
            RemoveMessagesFromStorage(tx, grouping.Key, grouping);
        }
        tx.Commit().ThrowOnError();
    }

    public ITransaction BeginTransaction()
    {
        return new LmdbTransaction(Environment);
    }

    public int FailedToSend(OutgoingMessage message)
    {
        using var tx = Environment.BeginTransaction();
        var result = FailedToSend(tx, message);
        tx.Commit().ThrowOnError();
        return result;
    }

    public void SuccessfullySent(IList<OutgoingMessage> messages)
    {
        using var tx = Environment.BeginTransaction();
        SuccessfullySent(tx, messages);
        tx.Commit().ThrowOnError();
    }

    public Message GetMessage(string queueName, MessageId messageId)
    {
        Span<byte> id = stackalloc byte[16];
        messageId.MessageIdentifier.TryWriteBytes(id);
        using var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
        var db = OpenDatabase(queueName);
        var result = tx.Get(db, id);
        var messageBuffer = result.value.AsSpan();
        return messageBuffer.ToMessage<Message>();
    }

    public string[] GetAllQueues()
    {
        return GetAllQueuesImpl().Where(x => OutgoingQueue != x).ToArray();
    }

    public void ClearAllStorage()
    {
        var databases = GetAllQueuesImpl().ToArray();
        using var tx = Environment.BeginTransaction();
        foreach (var databaseName in databases)
        {
            var db = OpenDatabase(databaseName);
            tx.TruncateDatabase(db);
        }
        tx.Commit().ThrowOnError();
    }

    private IEnumerable<string> GetAllQueuesImpl()
    {
        using var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
        using var db = tx.OpenDatabase();
        using var cursor = tx.CreateCursor(db);
        foreach (var (key, _) in cursor.AsEnumerable())
        {
            yield return Encoding.UTF8.GetString(key.CopyToNewArray());
        }
    }

    private void SuccessfullySent(LightningTransaction tx, IEnumerable<OutgoingMessage> messages)
    {
        RemoveMessagesFromStorage(tx, OutgoingQueue, messages);
    }
        
    public IEnumerable<Message> PersistedMessages(string queueName)
    {
        using var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
        var db = OpenDatabase(queueName);
        using var cursor = tx.CreateCursor(db);
        foreach (var (_, value) in cursor.AsEnumerable())
        {
            var valueSpan = value.AsSpan();
            var bytes = ArrayPool<byte>.Shared.Rent(valueSpan.Length);
            try
            {
                valueSpan.CopyTo(bytes);
                var reader = new SequenceReader(new ReadOnlySequence<byte>(bytes));
                var msg = reader.ReadMessage<Message>();
                yield return msg;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }
    }

    public IEnumerable<OutgoingMessage> PersistedOutgoingMessages()
    {
        using var tx = Environment.BeginTransaction(TransactionBeginFlags.ReadOnly);
        var db = OpenDatabase(OutgoingQueue);
        using var cursor = tx.CreateCursor(db);
        foreach (var (_, value) in cursor.AsEnumerable())
        {
            var valueSpan = value.AsSpan();
            var bytes = ArrayPool<byte>.Shared.Rent(valueSpan.Length);
            try
            {
                valueSpan.CopyTo(bytes);
                var reader = new SequenceReader(new ReadOnlySequence<byte>(bytes));
                var msg = reader.ReadOutgoingMessage();
                yield return msg; 
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }
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
        var db = OpenDatabase(message.Queue);
        RemoveMessageFromStorage(tx, db, message);
    }

    private void RemoveMessagesFromStorage<TMessage>(LightningTransaction tx, string queueName, IEnumerable<TMessage> messages)
        where TMessage : Message
    {
        var db = OpenDatabase(queueName);
        foreach (var message in messages)
        {
            RemoveMessageFromStorage(tx, db, message);
        }
    }

    private void RemoveMessageFromStorage<TMessage>(LightningTransaction tx, LightningDatabase db, TMessage message)
        where TMessage : Message
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        tx.Delete(db, id);
    }

    public void StoreOutgoing(ITransaction transaction, OutgoingMessage message)
    {
        var tx = ((LmdbTransaction) transaction).Transaction;
        StoreOutgoing(tx, message);
    }

    private void StoreOutgoing(LightningTransaction tx, OutgoingMessage message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = OpenDatabase(OutgoingQueue);
        tx.Put(db, id, message.AsReadOnlyMemory().Span);
    }

    private int FailedToSend(LightningTransaction tx, OutgoingMessage message)
    {
        Span<byte> id = stackalloc byte[16];
        message.Id.MessageIdentifier.TryWriteBytes(id);
        var db = OpenDatabase(OutgoingQueue);
        var value = tx.Get(db, id);
        if (value.resultCode == MDBResultCode.NotFound)
            return int.MaxValue;
        var msg = value.value.AsSpan().ToOutgoingMessage();
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
            tx.Put(db, id, message.AsReadOnlyMemory().Span);
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
            tx.Delete(original, id).ThrowOnError();
            tx.Put(newDb, id, message.AsReadOnlyMemory().Span).ThrowOnError();
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
        using var tx = Environment.BeginTransaction();
        var db = tx.OpenDatabase(queueName, new DatabaseConfiguration {Flags = DatabaseOpenFlags.Create});
        _databaseCache[queueName] = db;
        tx.Commit().ThrowOnError();
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
        Environment.Dispose();
    }
}