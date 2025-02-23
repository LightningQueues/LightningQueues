using System;
using System.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests.Storage.Lmdb;

[Collection("SharedTestDirectory")]
public class IncomingMessageScenarios : TestBase, IDisposable
{
    private readonly string _queuePath;
    private readonly LmdbMessageStore _store;

    public IncomingMessageScenarios(SharedTestDirectory testDirectory)
    {
        _queuePath = testDirectory.CreateNewDirectoryForTest();
        _store = new LmdbMessageStore(_queuePath, new MessageSerializer());
    }

    [Fact]
    public void happy_path_success()
    {
        var message = NewMessage();
        message.Headers.Add("my_key", "my_value");
        _store.CreateQueue(message.Queue);
        _store.StoreIncoming(message);
        var msg = _store.GetMessage(message.Queue, message.Id);
        System.Text.Encoding.UTF8.GetString(msg.Data).ShouldBe("hello");
        msg.Headers.First().Value.ShouldBe("my_value");
    }

    [Fact]
    public void storing_message_for_queue_that_doesnt_exist()
    {
        var message = NewMessage();
        Assert.Throws<QueueDoesNotExistException>(() =>
        {
            _store.StoreIncoming(message);
        });
    }

    [Fact]
    public void crash_before_commit()
    {
        var message = NewMessage();
        _store.CreateQueue(message.Queue);
        using (var transaction = _store.BeginTransaction())
        {
            _store.StoreIncoming(transaction, message);
            //crash
        }
        _store.Dispose();
        using var store = new LmdbMessageStore(_queuePath, new MessageSerializer());
        store.CreateQueue(message.Queue);
        var msg = store.GetMessage(message.Queue, message.Id);
        msg.ShouldBeNull();
    }

    [Fact]
    public void rollback_messages_received()
    {
        var message = NewMessage();
        _store.CreateQueue(message.Queue);
        using (var transaction = _store.BeginTransaction())
        {
            _store.StoreIncoming(transaction, message);
        }
        var msg = _store.GetMessage(message.Queue, message.Id);
        msg.ShouldBeNull();
    }

    [Fact]
    public void creating_multiple_stores()
    {
        _store.Dispose();
        var store = new LmdbMessageStore(_queuePath, new MessageSerializer());
        store.Dispose();
        using var store2 = new LmdbMessageStore(_queuePath, new MessageSerializer());
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        _store.Dispose();
    }
}