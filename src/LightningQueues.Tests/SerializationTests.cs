using System;
using System.Buffers;
using System.Collections.Generic;
using LightningQueues.Serialization;
using Shouldly;

namespace LightningQueues.Tests;

public class SerializationTests : TestBase
{
    public void can_serialize_and_deserialize_message_as_span()
    {
        var serializer = new MessageSerializer();
        var msg = Message.Create(
            data: "hello"u8.ToArray(),
            queue: "test",
            destinationUri: "lq.tcp://fake:1234"
        );
        var msgs = new List<Message> { msg };
        var memory = serializer.ToMemory(msgs);
        var serialized = new ReadOnlySequence<byte>(memory);
        var deserialized = serializer.ToMessage(serialized.Slice(sizeof(int)).FirstSpan);
        deserialized.Id.ShouldBe(msg.Id);
        deserialized.DataArray.ShouldBe(msg.DataArray);
    }

    public void comb_guid_generates_ascending_keys()
    {
        // Generate 1000 COMBs rapidly and verify they're in strictly ascending order
        var keys = new byte[1000][];
        for (var i = 0; i < 1000; i++)
        {
            var id = MessageId.GenerateRandom();
            keys[i] = new byte[16];
            id.MessageIdentifier.TryWriteBytes(keys[i]);
        }

        // Verify each consecutive pair is in ascending order (lexicographic comparison)
        for (var i = 1; i < keys.Length; i++)
        {
            var comparison = keys[i].AsSpan().SequenceCompareTo(keys[i - 1]);
            comparison.ShouldBeGreaterThan(0, $"Key at index {i} should be greater than key at index {i - 1}");
        }
    }

    public void wire_format_reader_extracts_routing_info_correctly()
    {
        var serializer = new MessageSerializer();
        var msg = Message.Create(
            data: "hello world"u8.ToArray(),
            queue: "myqueue",
            destinationUri: "lq.tcp://remotehost:5678/target"
        );

        // Serialize the message
        var serializedBytes = serializer.AsSpan(msg).ToArray();

        // Use WireFormatReader to extract routing info without full deserialization
        var rawMessage = WireFormatReader.ReadOutgoingMessage(serializedBytes, 0, serializedBytes.Length);

        // Verify extracted fields match original
        var extractedDestination = WireFormatReader.GetDestinationUri(in rawMessage);
        var extractedQueue = WireFormatReader.GetQueueName(in rawMessage);

        extractedDestination.ShouldBe("lq.tcp://remotehost:5678/target");
        extractedQueue.ShouldBe("myqueue");

        // Verify MessageId bytes match
        Span<byte> expectedMessageId = stackalloc byte[16];
        msg.Id.MessageIdentifier.TryWriteBytes(expectedMessageId);
        rawMessage.MessageId.Span.SequenceEqual(expectedMessageId).ShouldBeTrue();

        // Verify FullMessage contains the complete serialized data
        rawMessage.FullMessage.Length.ShouldBe(serializedBytes.Length);
    }

    public void wire_format_reader_handles_message_with_headers()
    {
        var serializer = new MessageSerializer();
        var headers = new Dictionary<string, string> { { "key1", "value1" }, { "key2", "value2" } };
        var msg = Message.Create(
            data: "test data"u8.ToArray(),
            queue: "headerqueue",
            destinationUri: "lq.tcp://host:1234",
            headers: headers
        );

        var serializedBytes = serializer.AsSpan(msg).ToArray();
        var rawMessage = WireFormatReader.ReadOutgoingMessage(serializedBytes, 0, serializedBytes.Length);

        WireFormatReader.GetDestinationUri(in rawMessage).ShouldBe("lq.tcp://host:1234");
        WireFormatReader.GetQueueName(in rawMessage).ShouldBe("headerqueue");
    }

    public void wire_format_reader_destination_comparison_without_allocation()
    {
        var serializer = new MessageSerializer();
        var msg1 = Message.Create(data: "a"u8.ToArray(), queue: "q", destinationUri: "lq.tcp://host:1234");
        var msg2 = Message.Create(data: "b"u8.ToArray(), queue: "q", destinationUri: "lq.tcp://host:1234");
        var msg3 = Message.Create(data: "c"u8.ToArray(), queue: "q", destinationUri: "lq.tcp://other:5678");

        var raw1 = WireFormatReader.ReadOutgoingMessage(serializer.AsSpan(msg1).ToArray(), 0, serializer.AsSpan(msg1).Length);
        var raw2 = WireFormatReader.ReadOutgoingMessage(serializer.AsSpan(msg2).ToArray(), 0, serializer.AsSpan(msg2).Length);
        var raw3 = WireFormatReader.ReadOutgoingMessage(serializer.AsSpan(msg3).ToArray(), 0, serializer.AsSpan(msg3).Length);

        // Same destination should be equal
        WireFormatReader.DestinationsEqual(in raw1, in raw2).ShouldBeTrue();

        // Different destinations should not be equal
        WireFormatReader.DestinationsEqual(in raw1, in raw3).ShouldBeFalse();

        // String comparison should work
        WireFormatReader.DestinationEquals(in raw1, "lq.tcp://host:1234").ShouldBeTrue();
        WireFormatReader.DestinationEquals(in raw1, "lq.tcp://wrong:9999").ShouldBeFalse();
    }
}

