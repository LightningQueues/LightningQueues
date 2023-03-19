using Xunit;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Builders;
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
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        _protocol = new ReceivingProtocol(_store, new NoSecurity(),
            new Uri("lq.tcp://localhost"), _logger);
    }

    [Fact]
    public async ValueTask client_sending_negative_length_is_ignored()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        using var ms = new MemoryStream();
        var iterationContinued = false;
        ms.Write(BitConverter.GetBytes(-2), 0, 4);
        ms.Position = 0;
        var result = _protocol.ReceiveMessagesAsync(ms, cancellation.Token);
        await foreach (var _ in result.WithCancellation(cancellation.Token))
        {
            iterationContinued = true;
        }
        iterationContinued.ShouldBeFalse();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask handling_disconnects_mid_protocol_gracefully()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(5));
        ms.Write("Fake this shouldn't pass!!!!!"u8);
        //even though we're not 'disconnecting', by making writable false it achieves the same outcome
        using var mockStream = new MemoryStream(ms.ToArray(), false);
        var msgs = _protocol.ReceiveMessagesAsync(mockStream, cancellation.Token);
        await foreach (var _ in msgs.WithCancellation(cancellation.Token))
        {
        }
        _logger.ErrorMessages.ShouldNotBeEmpty();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask handling_valid_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await RunLengthTest(0, cancellation.Token);
        _logger.DebugMessages.Any(x => x.StartsWith("Received length")).ShouldBeTrue();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask sending_shorter_length_than_payload_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await RunLengthTest(-2, cancellation.Token);
        _logger.DebugMessages.Any(x => x.StartsWith("Received length")).ShouldBeTrue();
        _logger.ErrorMessages.ShouldContain("Error finishing protocol acknowledgement");
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask sending_longer_length_than_payload_length()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await RunLengthTest(5, cancellation.Token);
        _logger.ErrorMessages.ShouldNotBeEmpty();
        cancellation.Cancel();
    }

    private async ValueTask RunLengthTest(int differenceFromActualLength, CancellationToken token)
    {
        var message = new OutgoingMessage
        {
            Id = MessageId.GenerateRandom(),
            Data = "hello"u8.ToArray(),
            Queue = "test"
        };
        
        var bytes = new[] { message }.AsReadOnlyMemory();
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(bytes.Length + differenceFromActualLength), 0, 4);
        ms.Write(bytes.Span);
        ms.Position = 0;
        var msgs = _protocol.ReceiveMessagesAsync(ms, token);
        await foreach (var _ in msgs.WithCancellation(token))
        {
        }
    }

    [Fact]
    public async ValueTask sending_to_a_queue_that_doesnt_exist()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var message = new OutgoingMessage
        {
            Id = MessageId.GenerateRandom(),
            Data = "hello"u8.ToArray(),
            Queue = "test"
        };
        var bytes = new[] { message }.AsReadOnlyMemory();
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(bytes.Length), 0, 4);
        ms.Write(bytes.Span);
        ms.Position = 0;
        var msgs = _protocol.ReceiveMessagesAsync(ms, cancellation.Token);
        await foreach (var _ in msgs.WithCancellation(cancellation.Token))
        {
        }
        _logger.InfoMessages.ShouldContain($"Queue {message.Queue} not found for {message.Id}");
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask sending_data_that_is_cannot_be_deserialized()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        using var ms = new MemoryStream();
        ms.Write(BitConverter.GetBytes(16), 0, 4);
        ms.Write(Guid.NewGuid().ToByteArray(), 0, 16);
        ms.Position = 0;
        
        var msgs = _protocol.ReceiveMessagesAsync(ms, cancellation.Token);
        await foreach (var _ in msgs.WithCancellation(cancellation.Token))
        {
        }
        _logger.ErrorMessages.Any(x => x.StartsWith("Error reading messages")).ShouldBeTrue();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask supports_ability_to_cancel_for_slow_clients()
    {
        using var cancelSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        using var ms = new MemoryStream();
        var msgs = _protocol.ReceiveMessagesAsync(ms, cancelSource.Token);
        await Task.Delay(50, default);
        ms.Write(BitConverter.GetBytes(5));
        await foreach (var _ in msgs.WithCancellation(cancelSource.Token))
        {
        }
        cancelSource.IsCancellationRequested.ShouldBe(true);
        _logger.DebugMessages.ShouldBeEmpty();
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}