using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using LightningQueues.Logging;

namespace LightningQueues.Net.Tcp;

public class Receiver : IDisposable
{
    private readonly TcpListener _listener;
    private readonly IReceivingProtocol _protocol;
    private readonly ILogger _logger;
    private bool _disposed;
    private readonly Uri _localUri;
    private readonly object _lockObject;
        
    public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol, ILogger logger)
    {
        Endpoint = endpoint;
        _localUri = new Uri($"lq://localhost:{Endpoint.Port}");
        _protocol = protocol;
        _logger = logger;
        _listener = new TcpListener(Endpoint);
        _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _lockObject = new object();
    }

    public IPEndPoint Endpoint { get; }

    public async IAsyncEnumerable<Message> StartReceivingAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        StartListener();
        while (!cancellationToken.IsCancellationRequested && !_disposed)
        {
            var socket = await _listener.AcceptSocketAsync(cancellationToken);
            await using var stream = new NetworkStream(socket, false);
            await foreach (var msg in _protocol.ReceiveMessagesAsync(
                               socket.RemoteEndPoint, stream, cancellationToken))
            {
                yield return msg;
            }
        }
    }

    private void StartListener()
    {
        lock (_lockObject)
        {
            _listener.Start();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _logger.InfoFormat("Disposing TcpListener at {0}", Endpoint.Port);
        _disposed = true;
        _listener.Stop();
        GC.SuppressFinalize(this);
    }
}