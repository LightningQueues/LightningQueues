using System;
using System.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Storage.Lmdb;

public class LmdbMessageStoreTests : TestBase
{
    public void getting_all_queues()
    {
        StorageScenario(store =>
        {
            store.CreateQueue("test2");
            store.CreateQueue("test3");
            store.GetAllQueues().SequenceEqual(new[] { "test", "test2", "test3" }).ShouldBeTrue();
        });
    }

    public void clear_all_history_with_empty_dataset_doesnt_throw()
    {
        StorageScenario(store =>
        {
            store.ClearAllStorage();
        });
    }

    public void clear_all_history_with_persistent_data()
    {
        StorageScenario(store =>
        {
            var message = NewMessage("test");
            var outgoingMessage = NewMessage();
            outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
            outgoingMessage.SentAt = DateTime.Now;
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }

            store.PersistedIncoming("test").Count().ShouldBe(1);
            store.PersistedOutgoing().Count().ShouldBe(1);
            store.ClearAllStorage();
            store.PersistedIncoming("test").Count().ShouldBe(0);
            store.PersistedOutgoing().Count().ShouldBe(0);
        });
    }

    public void store_can_read_previously_stored_items()
    {
        StorageScenario(store =>
        {
            var message = NewMessage("test");
            var outgoingMessage = NewMessage();
            outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
            outgoingMessage.SentAt = DateTime.Now;
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }

            store.Dispose();
            using var store2 = new LmdbMessageStore(store.Path, new MessageSerializer());
            store2.CreateQueue("test");
            store2.PersistedIncoming("test").Count().ShouldBe(1);
            store2.PersistedOutgoing().Count().ShouldBe(1);
        });
    }

    public void retrieve_message_by_id()
    {
        StorageScenario(store =>
        {
            var message = NewMessage("test");
            var outgoingMessage = NewMessage();
            outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
            outgoingMessage.SentAt = DateTime.Now;
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, outgoingMessage);
                store.StoreIncoming(tx, message);
                tx.Commit();
            }

            var message2 = store.GetMessage(message.Queue, message.Id);
            var outgoing2 = store.GetMessage("outgoing", outgoingMessage.Id);
            message2.ShouldNotBeNull();
            outgoing2.ShouldNotBeNull();
        });
    }
}