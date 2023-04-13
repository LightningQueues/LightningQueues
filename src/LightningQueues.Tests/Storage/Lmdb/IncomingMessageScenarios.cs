using System;
using System.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests.Storage.Lmdb;

[Collection("SharedTestDirectory")]
public class IncomingMessageScenarios : IDisposable
{
    private readonly string _queuePath;
    private LmdbMessageStore _store;

    public IncomingMessageScenarios(SharedTestDirectory testDirectory)
    {
        _queuePath = testDirectory.CreateNewDirectoryForTest();
        _store = new LmdbMessageStore(_queuePath, new MessageSerializer());
    }

    [Fact]
    public void happy_path_success()
    {
        var message = NewMessage<Message>();
        message.Headers.Add("my_key", "my_value");
        _store.CreateQueue(message.Queue);
        _store.StoreIncomingMessage(message);
        var msg = _store.GetMessage(message.Queue, message.Id);
        System.Text.Encoding.UTF8.GetString(msg.Data).ShouldBe("hello");
        msg.Headers.First().Value.ShouldBe("my_value");
    }

    [Fact]
    public void storing_message_for_queue_that_doesnt_exist()
    {
        var message = NewMessage<Message>();
        Assert.Throws<QueueDoesNotExistException>(() =>
        {
            _store.StoreIncomingMessage(message);
        });
    }

    [Fact]
    public void crash_before_commit()
    {
        var message = NewMessage<Message>();
        _store.CreateQueue(message.Queue);
        using (var transaction = _store.BeginTransaction())
        {
            _store.StoreIncomingMessage(transaction, message);
            _store.Dispose();
            //crash
        }
        _store = new LmdbMessageStore(_queuePath, new MessageSerializer());
        _store.CreateQueue(message.Queue);
        var msg = _store.GetMessage(message.Queue, message.Id);
        msg.ShouldBeNull();
    }

    [Fact]
    public void rollback_messages_received()
    {
        var message = NewMessage<Message>();
        _store.CreateQueue(message.Queue);
        var transaction = _store.BeginTransaction();
        _store.StoreIncomingMessage(transaction, message);
        transaction.Dispose();

        var msg = _store.GetMessage(message.Queue, message.Id);
        msg.ShouldBeNull();
    }

    [Fact]
    public void creating_multiple_stores()
    {
        _store.Dispose();
        _store = new LmdbMessageStore(_queuePath, new MessageSerializer());
        _store.Dispose();
        _store = new LmdbMessageStore(_queuePath, new MessageSerializer());
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}