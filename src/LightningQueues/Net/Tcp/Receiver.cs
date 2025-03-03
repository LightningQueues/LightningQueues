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
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.ReceiverAcceptError(ex);
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
        
        // Mark as disposed first to stop accepting new connections in StartReceivingAsync
        _disposed = true;
        _logger.ReceiverDisposing();
        
        try
        {
            // Safely stop the listener if it's running
            if(_listener.Server.IsBound)
                _listener.Stop();
        }
        catch (Exception ex)
        {
            // Just log and continue with disposal
            _logger.ReceiverDisposalError(ex);
        }
        
        GC.SuppressFinalize(this);
    }
}