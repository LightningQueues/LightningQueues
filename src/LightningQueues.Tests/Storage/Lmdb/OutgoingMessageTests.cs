using System;
using System.Collections.Generic;
using System.Linq;
using LightningQueues.Serialization;
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

    public void persisted_outgoing_raw_returns_wire_format_with_routing_info()
    {
        StorageScenario(store =>
        {
            var headers = new Dictionary<string, string> { ["key1"] = "value1" };
            var message1 = Message.Create(
                data: "hello"u8.ToArray(),
                queue: "queue1",
                destinationUri: "lq.tcp://host1:5050"
            );
            var message2 = Message.Create(
                data: "world"u8.ToArray(),
                queue: "queue2",
                destinationUri: "lq.tcp://host2:6060",
                headers: headers
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message1);
                store.StoreOutgoing(tx, message2);
                tx.Commit();
            }

            var rawMessages = store.PersistedOutgoingRaw().ToList();

            rawMessages.Count.ShouldBe(2);

            // Verify first message
            var raw1 = rawMessages[0];
            WireFormatReader.GetQueueName(in raw1).ShouldBe("queue1");
            WireFormatReader.GetDestinationUri(in raw1).ShouldBe("lq.tcp://host1:5050");
            raw1.FullMessage.Length.ShouldBeGreaterThan(0);

            // Verify MessageId matches
            Span<byte> expectedId1 = stackalloc byte[16];
            message1.Id.MessageIdentifier.TryWriteBytes(expectedId1);
            raw1.MessageId.Span.SequenceEqual(expectedId1).ShouldBeTrue();

            // Verify second message
            var raw2 = rawMessages[1];
            WireFormatReader.GetQueueName(in raw2).ShouldBe("queue2");
            WireFormatReader.GetDestinationUri(in raw2).ShouldBe("lq.tcp://host2:6060");
            raw2.FullMessage.Length.ShouldBeGreaterThan(0);

            // Verify MessageId matches
            Span<byte> expectedId2 = stackalloc byte[16];
            message2.Id.MessageIdentifier.TryWriteBytes(expectedId2);
            raw2.MessageId.Span.SequenceEqual(expectedId2).ShouldBeTrue();
        });
    }
}