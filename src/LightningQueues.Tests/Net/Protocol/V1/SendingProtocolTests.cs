using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Net.Protocol.V1;

public class SendingProtocolTests : TestBase
{

    public async Task writing_single_message()
    {
        var serializer = new MessageSerializer();
        using var store = new LmdbMessageStore(TempPath(), serializer);
        var sender = new SendingProtocol(store, new NoSecurity(), serializer, new RecordingLogger(Console));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var expected = NewMessage();
        expected.Destination = new Uri("lq.tcp://fake:1234");
        using var ms = new MemoryStream();
        //not exercising full protocol
        await Should.ThrowAsync<ProtocolViolationException>(async () =>
            await sender.SendAsync(new Uri("lq.tcp://localhost:5050"), ms, [expected], cancellation.Token));
        var bytes = new ReadOnlySequence<byte>(ms.ToArray());
        var msg = serializer.ToMessage(bytes.Slice(sizeof(int) * 2).FirstSpan);
        msg.Id.ShouldBe(expected.Id);
        await cancellation.CancelAsync();
    }
}