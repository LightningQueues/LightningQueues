using System;
using System.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests.Storage.Lmdb;

public class IncomingMessageScenarios : TestBase
{
    [Fact]
    public void happy_path_success()
    {
        StorageScenario(store =>
        {
            var message = NewMessage();
            message.Headers.Add("my_key", "my_value");
            store.CreateQueue(message.Queue);
            store.StoreIncoming(message);
            var msg = store.GetMessage(message.Queue, message.Id);
            System.Text.Encoding.UTF8.GetString(msg.Data).ShouldBe("hello");
            msg.Headers.First().Value.ShouldBe("my_value");
        });
    }

    [Fact]
    public void storing_message_for_queue_that_doesnt_exist()
    {
        StorageScenario(store =>
        {
            var message = NewMessage("blah");
            Assert.Throws<QueueDoesNotExistException>(() => { store.StoreIncoming(message); });
        });
    }

    [Fact]
    public void crash_before_commit()
    {
        StorageScenario(store =>
        {
            var message = NewMessage();
            store.CreateQueue(message.Queue);
            using (var transaction = store.BeginTransaction())
            {
                store.StoreIncoming(transaction, message);
                //crash
            }

            store.Dispose();
            using var store2 = new LmdbMessageStore(store.Path, new MessageSerializer());
            store2.CreateQueue(message.Queue);
            var msg = store2.GetMessage(message.Queue, message.Id);
            msg.ShouldBeNull();
        });

    }

    [Fact]
    public void rollback_messages_received()
    {
        StorageScenario(store =>
        {
            var message = NewMessage();
            store.CreateQueue(message.Queue);
            using (var transaction = store.BeginTransaction())
            {
                store.StoreIncoming(transaction, message);
            }

            var msg = store.GetMessage(message.Queue, message.Id);
            msg.ShouldBeNull();
        });
    }

    [Fact]
    public void creating_multiple_stores()
    {
        StorageScenario(store =>
        {
            store.Dispose();
            var store2 = new LmdbMessageStore(store.Path, new MessageSerializer());
            store.Dispose();
            using var store3 = new LmdbMessageStore(store.Path, new MessageSerializer());
        });

    }

    private void StorageScenario(Action<LmdbMessageStore> action)
    {
        using var store = new LmdbMessageStore(TempPath(), new MessageSerializer());
        store.CreateQueue("test");
        action(store);
    }
}