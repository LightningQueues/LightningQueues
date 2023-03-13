using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using DotNext.IO;
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
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        _sender = new SendingProtocol(_store, new NoSecurity(), new RecordingLogger());
    }

    [Fact]
    public async Task writing_single_message()
    {
        var expected = NewMessage<OutgoingMessage>();
        expected.Destination = new Uri("lq.tcp://fake:1234");
        using var ms = new MemoryStream();
        //not exercising full protocol
        await Assert.ThrowsAsync<ProtocolViolationException>(async () =>
            await _sender.SendAsync(new Uri("lq.tcp://localhost:5050"),
                ms, new[] { expected }, default));
        var bytes = new ReadOnlySequence<byte>(ms.ToArray());
        var reader = new SequenceReader(bytes);
        reader.ReadInt32(true); //ignore payload length
        reader.ReadInt32(true); //ignore message count
        var msg = reader.ReadOutgoingMessage();
        msg.Id.ShouldBe(expected.Id);
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}