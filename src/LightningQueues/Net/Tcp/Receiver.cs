using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net.Tcp;

public class Receiver : IDisposable
{
    private readonly TcpListener _listener;
    private readonly IReceivingProtocol _protocol;
    private readonly ILogger _logger;
    private bool _disposed;
        
    public Receiver(IPEndPoint endpoint, IReceivingProtocol protocol, ILogger logger)
    {
        Endpoint = endpoint;
        _protocol = protocol;
        _logger = logger;
        _listener = new TcpListener(Endpoint);
        _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
    }

    public IPEndPoint Endpoint { get; }

    public async ValueTask StartReceivingAsync(ChannelWriter<Message> receivedChannel, CancellationToken cancellationToken = default)
    {
        _listener.Start();
        try
        {
            while (!cancellationToken.IsCancellationRequested && !_disposed)
            {
                try
                {
                    using var socket = await _listener.AcceptSocketAsync(cancellationToken).ConfigureAwait(false);
                    await using var stream = new NetworkStream(socket, false);
                    try
                    {
                        var messages = await _protocol.ReceiveMessagesAsync(stream, cancellationToken)
                            .ConfigureAwait(false);
                        foreach (var msg in messages)
                        {
                            await receivedChannel.WriteAsync(msg, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.ReceiverErrorReadingMessages(socket.RemoteEndPoint, ex);
                    }
                }
                catch (Exception ex)
                {
                    if (_logger.IsEnabled(LogLevel.Error))
                        _logger.LogError(ex, "Error accepting socket");
                }
            }
        }
        finally
        {
            _listener.Stop();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        
        _logger.ReceiverDisposing();
        _disposed = true;
        if(_listener.Server.IsBound)
            _listener.Stop();
        GC.SuppressFinalize(this);
    }
}