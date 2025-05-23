using System;
using System.Collections.Generic;
using System.Linq;
using Shouldly;

namespace LightningQueues.Tests.Storage.Lmdb;

public class OutgoingMessageTests : TestBase
{
    public void happy_path_messages_sent()
    {
        StorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050"
            );
            var headers = new Dictionary<string, string> { ["header"] = "header_value" };
            var message2 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                deliverBy: DateTime.Now.AddSeconds(5),
                maxAttempts: 3,
                headers: headers
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                store.StoreOutgoing(tx, message2);
                tx.Commit();
            }

            store.SuccessfullySent(message);

            var result = store.PersistedOutgoing().First();
            result.Id.ShouldBe(message2.Id);
            result.QueueString.ShouldBe(message2.QueueString);
            result.DataArray.ShouldBe(message2.DataArray);
            result.SentAt.ShouldBe(message2.SentAt);
            result.DeliverBy.ShouldBe(message2.DeliverBy);
            result.MaxAttempts.ShouldBe(message2.MaxAttempts);
            result.GetHeadersDictionary()["header"].ShouldBe("header_value");
        });
    }

    public void failed_to_send_with_max_attempts()
    {
        StorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                maxAttempts: 1
            ).WithSentAttempts(1);
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(false, message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }

    public void failed_to_send_with_deliver_by()
    {
        StorageScenario(store =>
        {
            var message = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "test",
                destinationUri: "lq.tcp://localhost:5050",
                deliverBy: DateTime.Now
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            store.FailedToSend(false, message);

            var outgoing = store.GetMessage("outgoing", message.Id);
            outgoing.ShouldBeNull();
        });
    }
}