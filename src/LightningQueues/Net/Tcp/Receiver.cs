using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using LightningQueues.Logging;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;

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
        while (!cancellationToken.IsCancellationRequested && IsNotDisposed())
        {
            var socket = await _listener.AcceptSocketAsync(cancellationToken);
            await using var stream = new NetworkStream(socket, false);
            await foreach (var msg in _protocol.ReceiveMessagesAsync(
                               socket.RemoteEndPoint, stream, cancellationToken))
            {
                yield return msg;
            }
        }
        Dispose();
    }

    private void StartListener()
    {
        lock (_lockObject)
        {
            _listener.Start();
        }
    }

    public IObservable<Message> StartReceiving()
    {
        return Observable.Empty<Message>();
        // lock (_lockObject)
        // {
        //     if (_stream != null)
        //         return _stream;
        //
        //     _listener.Start();
        //
        //     _logger.DebugFormat("TcpListener started listening on port: {0}", Endpoint.Port);
        //     _stream = Observable.While(IsNotDisposed, ContinueAcceptingNewClients())
        //         .Using(x => _protocol.ReceiveStream(
        //                 _security.Apply(_localUri,  Observable.Return(new NetworkStream(x, false))), 
        //                 x.RemoteEndPoint.ToString())
        //             .Catch((Exception ex) => CatchAll(ex)))
        //         .Catch((Exception ex) => CatchAll(ex))
        //         .Publish()
        //         .RefCount()
        //         .Finally(() => _logger.InfoFormat("TcpListener at {0} has stopped", Endpoint.Port))
        //         .Catch((Exception ex) => CatchAll(ex));
        // }
        // return _stream;
    }

    private IObservable<Message> CatchAll(Exception ex)
    {
        _logger.Error("Error in message receiving", ex);
        return Observable.Empty<Message>();
    }

    private bool IsNotDisposed()
    {
        return !_disposed;
    }

    private IObservable<Socket> ContinueAcceptingNewClients()
    {
        return Observable.FromAsync(() => _listener.AcceptSocketAsync())
            .Do(x => _logger.DebugFormat("Client at {0} connection established.", x.RemoteEndPoint))
            .Repeat();
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