using System;
using System.Linq;
using Shouldly;

namespace LightningQueues.Tests.Storage.Lmdb;

public class OutgoingMessageTests : TestBase
{
    public void happy_path_messages_sent()
    {
        StorageScenario(store =>
        {
            var destination = new Uri("lq.tcp://localhost:5050");
            var message = NewMessage("test");
            message.Destination = destination;
            var message2 = NewMessage("test");
            message2.Destination = destination;
            message2.DeliverBy = DateTime.Now.AddSeconds(5);
            message2.MaxAttempts = 3;
            message2.Headers.Add("header", "header_value");
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                store.StoreOutgoing(tx, message2);
                tx.Commit();
            }

            store.SuccessfullySent(message);

            var result = store.PersistedOutgoing().First();
            result.Id.ShouldBe(message2.Id);
            result.Queue.ShouldBe(message2.Queue);
            result.Data.ShouldBe(message2.Data);
            result.SentAt.ShouldBe(message2.SentAt);
            result.DeliverBy.ShouldBe(message2.DeliverBy);
            result.MaxAttempts.ShouldBe(message2.MaxAttempts);
            result.Headers["header"].ShouldBe("header_value");
        });
    }

    public void failed_to_send_with_max_attempts()
    {
        StorageScenario(store =>
        {
            var destination = new Uri("lq.tcp://localhost:5050");
            var message = NewMessage("test");
            message.MaxAttempts = 1;
            message.SentAttempts = 1;
            message.Destination = destination;
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }

    public void failed_to_send_with_deliver_by()
    {
        StorageScenario(store =>
        {
            var destination = new Uri("lq.tcp://localhost:5050");
            var message = NewMessage("test");
            message.DeliverBy = DateTime.Now;
            message.Destination = destination;
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }
}