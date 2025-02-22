using Xunit;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Storage;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Net.Protocol.V1;

[Collection("SharedTestDirectory")]
public class ReceivingProtocolTests : IDisposable
{
    private readonly RecordingLogger _logger;
    private readonly ReceivingProtocol _protocol;
    private readonly IMessageStore _store;

    public ReceivingProtocolTests(SharedTestDirectory testDirectory)
    {
        _logger = new RecordingLogger();
        var serializer = new MessageSerializer();
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest(), serializer);
        _store.CreateQueue("test");
        _protocol = new ReceivingProtocol(_store, new NoSecurity(), serializer, new Uri("lq.tcp://localhost"), _logger);
    }

    [Fact]
    public async Task client_sending_negative_length_is_ignored()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(-2), 0, 4);
        ms.Position = 0;
        var result = await _protocol.ReceiveMessagesAsync(ms, cancellation.Token);
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task handling_disconnects_mid_protocol_gracefully()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(29));
        ms.Write("Fake this shouldn't pass!!!!!"u8);
        //even though we're not 'disconnecting', by making writable false it achieves the same outcome
        using var mockStream = new MemoryStream(ms.ToArray(), false);
        await Should.ThrowAsync<ProtocolViolationException>(async () =>
            await _protocol.ReceiveMessagesAsync(mockStream, cancellation.Token));
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task handling_valid_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await RunLengthTest(0, cancellation.Token);
        _logger.DebugMessages.Any(x => x.StartsWith("Received length")).ShouldBeTrue();
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task sending_shorter_length_than_payload_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var ex = await Assert.ThrowsAsync<ProtocolViolationException>(async () => await RunLengthTest(-2, cancellation.Token));
        ex.Message.ShouldBe("Protocol violation: received length of 70 bytes, but 72 bytes were available");
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task sending_longer_length_than_payload_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var ex = await Assert.ThrowsAsync<ProtocolViolationException>(async () => await RunLengthTest(5, cancellation.Token));
        ex.Message.ShouldBe("Protocol violation: received length of 77 bytes, but 72 bytes were available");
        await cancellation.CancelAsync();
    }

    private async Task RunLengthTest(int differenceFromActualLength, CancellationToken token)
    {
        var message = new Message
        {
            Id = MessageId.GenerateRandom(),
            Data = "hello"u8.ToArray(),
            Queue = "test"
        };
        var serializer = new MessageSerializer();
        var memory = serializer.ToMemory(new List<Message> { message });
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(memory.Length + differenceFromActualLength), 0, 4);
        ms.Write(memory.Span);
        ms.Position = 0;
        var msgs = await _protocol.ReceiveMessagesAsync(ms, token);
    }

    [Fact]
    public async Task sending_to_a_queue_that_doesnt_exist()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var message = new Message
        {
            Id = MessageId.GenerateRandom(),
            Data = "hello"u8.ToArray(),
            Queue = "test2"
        };
        var serializer = new MessageSerializer();
        var memory = serializer.ToMemory(new List<Message>{ message });
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(memory.Length), 0, 4);
        ms.Write(memory.Span);
        ms.Position = 0;
        await Should.ThrowAsync<QueueDoesNotExistException>(async Task () =>
        {
            await _protocol.ReceiveMessagesAsync(ms, cancellation.Token);
        });
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task sending_data_that_is_cannot_be_deserialized()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(3));
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
        
        var ex = await Should.ThrowAsync<ProtocolViolationException>(async () => 
            await _protocol.ReceiveMessagesAsync(ms, cancellation.Token));
        await cancellation.CancelAsync();
    }

    [Fact]
    public async Task supports_ability_to_cancel_for_slow_clients()
    {
        using var cancelSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        using var ms = new MemoryStream();
        var msgs = _protocol.ReceiveMessagesAsync(ms, cancelSource.Token);
        await Task.Delay(50, CancellationToken.None);
        ms.Write(BitConverter.GetBytes(5));
        cancelSource.IsCancellationRequested.ShouldBe(true);
        _logger.DebugMessages.ShouldBeEmpty();
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}