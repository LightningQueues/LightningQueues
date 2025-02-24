using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Net;
using LightningQueues.Storage;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Net.Protocol.V1;

public class ReceivingProtocolTests : TestBase
{
    public async Task client_sending_negative_length_is_ignored()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(-2), 0, 4);
            ms.Position = 0;
            var result = await protocol.ReceiveMessagesAsync(ms, token);
        });
    }

    public async Task handling_disconnects_mid_protocol_gracefully()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(29));
            ms.Write("Fake this shouldn't pass!!!!!"u8);
            //even though we're not 'disconnecting', by making writable false it achieves the same outcome
            using var mockStream = new MemoryStream(ms.ToArray(), false);
            await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await protocol.ReceiveMessagesAsync(mockStream, token));
        });
    }

    public async Task handling_valid_length()
    {
        await ReceivingScenario(async (protocol, logger, token) =>
        {
            await RunLengthTest(protocol, 0, token);
            logger.DebugMessages.Any(x => x.StartsWith("Received length")).ShouldBeTrue();
        });
    }

    public async Task sending_shorter_length_than_payload_length()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            var ex = await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await RunLengthTest(protocol, -2, token));
            ex.Message.ShouldBe("Protocol violation: received length of 70 bytes, but 72 bytes were available");
        });
    }

    public async Task sending_longer_length_than_payload_length()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            var ex = await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await RunLengthTest(protocol, 5, token));
            ex.Message.ShouldBe("Protocol violation: received length of 77 bytes, but 72 bytes were available");
        });
    }

    private async Task RunLengthTest(IReceivingProtocol protocol, int differenceFromActualLength, CancellationToken token)
    {
        var message = new Message
        {
            Id = MessageId.GenerateRandom(),
            Data = "hello"u8.ToArray(),
            Queue = "test"
        };
        var serializer = new MessageSerializer();
        var memory = serializer.ToMemory([message]);
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(memory.Length + differenceFromActualLength), 0, 4);
        ms.Write(memory.Span);
        ms.Position = 0;
        var msgs = await protocol.ReceiveMessagesAsync(ms, token);
    }

    public async Task sending_to_a_queue_that_doesnt_exist()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            var message = new Message
            {
                Id = MessageId.GenerateRandom(),
                Data = "hello"u8.ToArray(),
                Queue = "test2"
            };
            var serializer = new MessageSerializer();
            var memory = serializer.ToMemory([message]);
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(memory.Length), 0, 4);
            ms.Write(memory.Span);
            ms.Position = 0;
            await Should.ThrowAsync<QueueDoesNotExistException>(async Task () =>
            {
                await protocol.ReceiveMessagesAsync(ms, token);
            });
        });
    }

    public async Task sending_data_that_is_cannot_be_deserialized()
    {
        await ReceivingScenario(async (protocol, _, token) =>
        {
            using var ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(215), 0, 4);
            ms.Write(Guid.NewGuid().ToByteArray(), 0, 16);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("sdlfjaslkdjfalsdjfalsjdfasjdflk;asjdfjdasdfaskldfjjflsjfklsjl"u8);
            ms.Write("blah"u8);
            ms.Write("yah"u8);
            ms.Write("tah"u8);
            ms.Write("fah"u8);
            ms.Write("wah"u8);
            ms.Position = 0;

            await Should.ThrowAsync<ProtocolViolationException>(async () =>
                await protocol.ReceiveMessagesAsync(ms, token));
        });
    }

    public async Task supports_ability_to_cancel_for_slow_clients()
    {
        await ReceivingScenario(async (protocol, logger, token) =>
        {
            using var ms = new MemoryStream();
            var msgs = protocol.ReceiveMessagesAsync(ms, token);
            await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
            ms.Write(BitConverter.GetBytes(5));
            token.IsCancellationRequested.ShouldBe(true);
            logger.DebugMessages.ShouldBeEmpty();
        });
    }

    private async Task ReceivingScenario(Func<ReceivingProtocol, RecordingLogger, CancellationToken, Task> scenario)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var logger = new RecordingLogger();
        var serializer = new MessageSerializer();
        using var store = new LmdbMessageStore(TempPath(), serializer);
        store.CreateQueue("test");
        var protocol = new ReceivingProtocol(store, new NoSecurity(), serializer, new Uri("lq.tcp://localhost"), logger); 
        await scenario(protocol, logger, cancellation.Token);
        await cancellation.CancelAsync();
    }
}