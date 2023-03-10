using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net.Tcp;

public class Sender : IDisposable
{
    private readonly ISendingProtocol _protocol;
    private readonly Channel<OutgoingMessageFailure> _failedToSend;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cancellation;

    public Sender(ISendingProtocol protocol, ILogger logger)
    {
        _protocol = protocol;
        _logger = logger;
        _failedToSend = Channel.CreateUnbounded<OutgoingMessageFailure>();
        _cancellation = new CancellationTokenSource();
    }

    public Channel<OutgoingMessageFailure> FailedToSend() => _failedToSend;

    public async ValueTask StartSendingAsync(ChannelReader<OutgoingMessage> outgoing, CancellationToken cancellationToken)
    {
        var allOutgoing = outgoing.ReadAllAsync(cancellationToken);
        await foreach (var sendTo in allOutgoing.Buffer(TimeSpan.FromMilliseconds(200), 50, cancellationToken))
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, source.Token);
            var messages = sendTo.GroupBy(x => x.Destination);
            foreach (var messageGroup in messages)
            {
                var uri = messageGroup.Key;
                try
                {
                    var client = new TcpClient();
                    if (Dns.GetHostName() == uri.Host)
                    {
                        await client.ConnectAsync(IPAddress.Loopback, uri.Port, linked.Token);
                    }
                    else
                    {
                        await client.ConnectAsync(uri.Host, uri.Port, linked.Token);
                    }

                    await _protocol.SendAsync(uri, client.GetStream(), messageGroup, linked.Token);
                }
                catch (QueueDoesNotExistException ex)
                {
                    _logger.Error($"Queue does not exist at {uri}", ex);
                }
                catch (Exception ex)
                {
                    _logger.Error($"Failed to send messages to {uri}", ex);
                    var failed = new OutgoingMessageFailure
                    {
                        Batch = new OutgoingMessageBatch(uri, messageGroup, new TcpClient(), null)
                    };
                    await _failedToSend.Writer.WriteAsync(failed, cancellationToken);
                }
            }
        }
    }

    public void Dispose()
    {
        _logger.Info("Disposing Sender to ");
        if (_cancellation.IsCancellationRequested) return;
        _cancellation.Cancel();
        GC.SuppressFinalize(this);
    }
}