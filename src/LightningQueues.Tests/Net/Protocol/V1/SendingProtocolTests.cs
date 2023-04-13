using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Builders;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests.Net.Protocol.V1;

[Collection("SharedTestDirectory")]
public class SendingProtocolTests : IDisposable
{
    private readonly SendingProtocol _sender;
    private readonly IMessageStore _store;

    public SendingProtocolTests(SharedTestDirectory testDirectory)
    {
        var serializer = new MessageSerializer();
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest(), serializer);
        _sender = new SendingProtocol(_store, new NoSecurity(), serializer, new RecordingLogger());
    }

    [Fact]
    public async Task writing_single_message()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var expected = NewMessage<Message>();
        expected.Destination = new Uri("lq.tcp://fake:1234");
        using var ms = new MemoryStream();
        //not exercising full protocol
        await Assert.ThrowsAsync<ProtocolViolationException>(async () =>
            await _sender.SendAsync(new Uri("lq.tcp://localhost:5050"),
                ms, new[] { expected }, cancellation.Token));
        var bytes = new ReadOnlySequence<byte>(ms.ToArray());
        var serializer = new MessageSerializer();
        var msg = serializer.ToMessage(bytes.Slice(sizeof(int) * 2).FirstSpan);
        msg.Id.ShouldBe(expected.Id);
        cancellation.Cancel();
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}