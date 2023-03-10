﻿using System;
using System.Linq;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Storage;
using Xunit;

namespace LightningQueues.Tests.Storage;

public class NoStorageIntegrationTester : IDisposable
{
    private readonly Queue _sender;
    private readonly Queue _receiver;

    public NoStorageIntegrationTester()
    {
        _sender = ObjectMother.NewQueue(logger: new NulloLogger(), store:new NoStorage());
        _receiver = ObjectMother.NewQueue(logger: new NulloLogger(), store:new NoStorage());
    }

    [Fact]
    public async Task can_send_and_receive_without_storage()
    {
        var taskCompletionSource = new TaskCompletionSource<bool>();
        var receiveTask = _receiver.Receive("test").FirstAsync();

        var destination = new Uri($"lq.tcp://localhost:{_receiver.Endpoint.Port}");
        var message = ObjectMother.NewMessage<OutgoingMessage>("test");
        message.Destination = destination;
        _sender.Send(message);
        await receiveTask;
    }

    public void Dispose()
    {
        _sender.Dispose();
        _receiver.Dispose();
        GC.SuppressFinalize(this);
    }
}