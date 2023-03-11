using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;

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
        var expected = ObjectMother.NewMessage<OutgoingMessage>();
        using var ms = new MemoryStream();
        //not exercising full protocol
        await Assert.ThrowsAsync<ProtocolViolationException>(async () =>
            await _sender.SendAsync(new Uri("lq.tcp://localhost:5050"),
                ms, new[] { expected }, default));
        var bytes = ms.ToArray();
        var msg = bytes.ToMessages().First();
        msg.Id.ShouldBe(expected.Id);
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}