using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Net.Tcp;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Net.Tcp;

public class ReceiverTests : TestBase
{
    public async Task stops_listening_on_task_cancellation()
    {
        await NetworkScenario(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            var listener = new TcpListener(endpoint);
            Should.Throw<SocketException>(() => listener.Start());
            await token.CancelAsync();
            await DeterministicDelay(500, CancellationToken.None);
            receivingLoop.IsCompleted.ShouldBe(true);
            listener.Start();
            listener.Stop();
        });
    }


    public async Task can_handle_connect_then_disconnect()
    {
        await NetworkScenario(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            using (var client = new TcpClient())
            {
                await client.ConnectAsync(endpoint.Address, endpoint.Port, token.Token);
            }
            receivingLoop.IsFaulted.ShouldBeFalse();
        });
    }

    public async Task can_handle_sending_three_bytes_then_disconnect()
    {
        await NetworkScenario(async (endpoint, _, _, token, receivingLoop, _) =>
        {
            using (var client = new TcpClient())
            {
                await client.ConnectAsync(endpoint.Address, endpoint.Port, token.Token);
                await client.GetStream().WriteAsync((new byte[] { 1, 4, 6 }).AsMemory(0, 3), token.Token);
            }
            receivingLoop.IsFaulted.ShouldBeFalse();
        });
    }

    public async Task accepts_concurrently_connected_clients()
    {
        await NetworkScenario(async (endpoint, _, _, token, receivingTask, _) =>
        {
            using var client1 = new TcpClient();
            using var client2 = new TcpClient();
            await client1.ConnectAsync(endpoint.Address, endpoint.Port, token.Token);
            await client2.ConnectAsync(endpoint.Address, endpoint.Port, token.Token);
            await client2.GetStream()
                .WriteAsync(new byte[] { 1, 4, 6 }.AsMemory(0, 3), token.Token);
            await client1.GetStream()
                .WriteAsync(new byte[] { 1, 4, 6 }.AsMemory(0, 3), token.Token);
            receivingTask.IsFaulted.ShouldBeFalse();
        });
    }

    public async Task receiving_a_valid_message()
    {
        var expected = NewMessage("test");
        await NetworkScenario(async (endpoint, sender, _, cancellation, _, channel) =>
        {
            var messages = new List<Message>([expected]);
            using var client = new TcpClient();
            await client.ConnectAsync(endpoint.Address, endpoint.Port, cancellation.Token);
            await sender.SendAsync(expected.Destination, client.GetStream(), messages, cancellation.Token);

            var actual = await channel.Reader.ReadAsync(cancellation.Token);
            await cancellation.CancelAsync();
            actual.Id.ShouldBe(expected.Id);
            actual.QueueString.ShouldBe(expected.QueueString);
            Encoding.UTF8.GetString(actual.DataArray).ShouldBe("hello");
        }, expected);

    }

    private async Task NetworkScenario(Func<IPEndPoint, SendingProtocol, Receiver, CancellationTokenSource, Task, Channel<Message>, Task> scenario, Message? expected = null)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var endpoint = new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort());
        var logger = new RecordingLogger(Console);
        var serializer = new MessageSerializer();
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, serializer);
        store.CreateQueue("test");
        using var sendingEnv = LightningEnvironment();
        using var sendingStore = new LmdbMessageStore(sendingEnv, serializer);
        sendingStore.CreateQueue("test");
        
        if (expected.HasValue)
        {
            var expectedWithDestination = new Message(
                expected.Value.Id,
                expected.Value.Data,
                expected.Value.Queue,
                expected.Value.SentAt,
                expected.Value.SubQueue,
                $"lq.tcp://localhost:{endpoint.Port}".AsMemory(),
                expected.Value.DeliverBy,
                expected.Value.MaxAttempts,
                expected.Value.Headers
            );
            using var tx = sendingStore.BeginTransaction();
            sendingStore.StoreOutgoing(tx, expectedWithDestination);
            tx.Commit();
        }

        var sender = new SendingProtocol(sendingStore, new NoSecurity(), serializer, logger);
        var protocol = new ReceivingProtocol(store, new NoSecurity(), serializer, 
            new Uri($"lq.tcp://localhost:{endpoint.Port}"), logger);
        using var receiver = new Receiver(endpoint, protocol, logger); 
        var channel = Channel.CreateUnbounded<Message>();
        var receivingTask = Task.Factory.StartNew(() => 
            receiver.StartReceivingAsync(channel.Writer, cancellation.Token), cancellation.Token);
        await DeterministicDelay(50, CancellationToken.None);
        await scenario(endpoint, sender, receiver, cancellation, receivingTask, channel);
        await cancellation.CancelAsync();
    }
}