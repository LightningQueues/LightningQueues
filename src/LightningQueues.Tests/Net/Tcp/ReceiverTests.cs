using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests.Net.Tcp;

[Collection("SharedTestDirectory")]
public class ReceiverTests : IDisposable
{
    private readonly IMessageStore _store;
    private readonly IMessageStore _sendingStore;
    private readonly SendingProtocol _sender;
    private readonly Receiver _receiver;
    private readonly IPEndPoint _endpoint;

    public ReceiverTests(SharedTestDirectory testDirectory)
    {
        var port = PortFinder.FindPort(); //to make it possible to run in parallel
        _endpoint = new IPEndPoint(IPAddress.Loopback, port);
        var logger = new RecordingLogger();
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        _store.CreateQueue("test");
        _sendingStore = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        _sendingStore.CreateQueue("test");
        _sender = new SendingProtocol(_sendingStore, new NoSecurity(), logger);
        var protocol = new ReceivingProtocol(_store, new NoSecurity()
            , new Uri($"lq.tcp://localhost:{_endpoint.Port}"), logger);
        _receiver = new Receiver(_endpoint, protocol, logger);
    }

    [Fact]
    public async ValueTask stops_listening_on_task_cancellation()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var receivingTask = Task.Factory.StartNew(async () =>
        {
            var channel = Channel.CreateUnbounded<Message>();
            await _receiver.StartReceivingAsync(channel.Writer, cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);
        await Task.Delay(100, default);
        var listener = new TcpListener(_endpoint);
        Assert.Throws<SocketException>(() => listener.Start());
        cancellationTokenSource.Cancel();
        await Task.Delay(100, default);
        listener.Start();
        listener.Stop();
    }


    [Fact]
    public async Task can_handle_connect_then_disconnect()
    {
        var receivingTask = Task.Factory.StartNew(async () =>
        {
            var channel = Channel.CreateUnbounded<Message>();
            await _receiver.StartReceivingAsync(channel.Writer, default);
        });
        await Task.Delay(100);
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(_endpoint.Address, _endpoint.Port);
        }
        receivingTask.IsFaulted.ShouldBeFalse();
    }

    [Fact]
    public async Task can_handle_sending_three_bytes_then_disconnect()
    {
        var receivingTask = Task.Factory.StartNew(async () =>
        {
            var channel = Channel.CreateUnbounded<Message>();
            await _receiver.StartReceivingAsync(channel.Writer, default);
        });
        await Task.Delay(100);
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(_endpoint.Address, _endpoint.Port);
            client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
        }
        receivingTask.IsFaulted.ShouldBeFalse();
    }

    [Fact]
    public async Task accepts_concurrently_connected_clients()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var receivingTask = Task.Factory.StartNew(async () =>
        {
            var channel = Channel.CreateUnbounded<Message>();
            await _receiver.StartReceivingAsync(channel.Writer, cancellationTokenSource.Token);
        }, cancellationTokenSource.Token);
        await Task.Delay(50, default);

        using var client1 = new TcpClient();
        using var client2 = new TcpClient();
        await client1.ConnectAsync(_endpoint.Address, _endpoint.Port, default);
        await client2.ConnectAsync(_endpoint.Address, _endpoint.Port, default);
        await client2.GetStream()
            .WriteAsync((new byte[] { 1, 4, 6 }).AsMemory(0, 3), default);
        await client1.GetStream()
            .WriteAsync((new byte[] { 1, 4, 6 }).AsMemory(0, 3), default);
        cancellationTokenSource.Cancel();
        await receivingTask;
    }

    [Fact]
    public async Task receiving_a_valid_message()
    {
        var expected = ObjectMother.NewMessage<OutgoingMessage>("test");
        Message actual = null;
        expected.Data = "hello"u8.ToArray();
        expected.Destination = new Uri($"lq.tcp://localhost:{_endpoint.Port}");
        var tx = _store.BeginTransaction();
        _sendingStore.StoreOutgoing(tx, expected);
        tx.Commit();
        var messages = new[] {expected};
        var receivingTask = Task.Factory.StartNew(async () =>
        {
            var channel = Channel.CreateUnbounded<Message>();
            _receiver.StartReceivingAsync(channel.Writer, default);
            actual = await channel.Reader.ReadAsync();
        });
        await Task.Delay(100);
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(_endpoint.Address, _endpoint.Port);
            await _sender.SendAsync(expected.Destination, client.GetStream(), messages, default);
        }

        await Task.Delay(100);
        actual.ShouldNotBeNull();
        actual.Id.ShouldBe(expected.Id);
        actual.Queue.ShouldBe(expected.Queue);
        Encoding.UTF8.GetString(actual.Data).ShouldBe("hello");
    }

    public void Dispose()
    {
        _receiver.Dispose();
        _store.Dispose();
        _sendingStore.Dispose();
        GC.SuppressFinalize(this);
    }
}