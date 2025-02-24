using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net.Tcp;

public class Sender : IDisposable
{
    private readonly ISendingProtocol _protocol;
    private readonly Channel<OutgoingMessageFailure> _failedToSend;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cancellation;
    private readonly TimeSpan _sendTimeout;

    public Sender(ISendingProtocol protocol, ILogger logger, TimeSpan sendTimeout)
    {
        _protocol = protocol;
        _logger = logger;
        _sendTimeout = sendTimeout;
        _failedToSend = Channel.CreateUnbounded<OutgoingMessageFailure>();
        _cancellation = new CancellationTokenSource();
    }

    public Channel<OutgoingMessageFailure> FailedToSend() => _failedToSend;

    public async ValueTask StartSendingAsync(ChannelReader<Message> outgoing, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var batch = await outgoing.ReadBatchAsync(50, TimeSpan.FromMilliseconds(200), cancellationToken)
                    .ConfigureAwait(false);
                using var source = new CancellationTokenSource(_sendTimeout);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, source.Token);
                foreach (var messageGroup in batch.GroupBy(x => x.Destination))
                {
                    var uri = messageGroup.Key;
                    var messages = messageGroup.ToList();
                    try
                    {
                        using var client = new TcpClient();
                        if (uri.IsLoopback || Dns.GetHostName() == uri.Host)
                        {
                            await client.ConnectAsync(IPAddress.Loopback, uri.Port, linked.Token)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            await client.ConnectAsync(uri.Host, uri.Port, linked.Token).ConfigureAwait(false);
                        }

                        await _protocol.SendAsync(uri, client.GetStream(), messages, linked.Token)
                            .ConfigureAwait(false);
                    }
                    catch (QueueDoesNotExistException ex)
                    {
                        _logger.SenderQueueDoesNotExistError(uri, ex);
                    }
                    catch (Exception ex)
                    {
                        _logger.SenderSendingError(uri, ex);
                        var failed = new OutgoingMessageFailure
                        {
                            Messages = messages
                        };
                        await _failedToSend.Writer.WriteAsync(failed, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.SenderSendingLoopError(ex);
            }
        }
    }

    public void Dispose()
    {
        _logger.SenderDisposing();
        using (_cancellation)
        {
            if (!_cancellation.IsCancellationRequested)
                _cancellation.Cancel();
        }

        GC.SuppressFinalize(this);
    }
}