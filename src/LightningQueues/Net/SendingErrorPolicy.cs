using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net;

public class SendingErrorPolicy
{
    private readonly ILogger _logger;
    private readonly IMessageStore _store;
    private readonly Channel<OutgoingMessageFailure> _failedToConnect;
    private readonly Channel<OutgoingMessage> _retries;

    public SendingErrorPolicy(ILogger logger, IMessageStore store, Channel<OutgoingMessageFailure> failedToConnect)
    {
        _logger = logger;
        _store = store;
        _failedToConnect = failedToConnect;
        _retries = Channel.CreateUnbounded<OutgoingMessage>();
    }

    public ChannelReader<OutgoingMessage> Retries => _retries.Reader;

    public async ValueTask StartRetries(CancellationToken cancellationToken)
    {
        await foreach (var messageFailure in _failedToConnect.Reader.ReadAllAsync(cancellationToken))
        {
            foreach (var message in messageFailure.Batch.Messages)
            {
                IncrementAttempt(message);
                if (!ShouldRetry(message)) 
                    continue;
                await Task.Delay(TimeSpan.FromSeconds(message.SentAttempts * message.SentAttempts), cancellationToken);
                await _retries.Writer.WriteAsync(message, cancellationToken);
            }
        }
    }

    public bool ShouldRetry(OutgoingMessage message)
    {
        var totalAttempts = message.MaxAttempts ?? 100;
        _logger.DebugFormat("Failed to send should retry with AttemptCount: {0}, TotalAttempts {1}", message.SentAttempts, totalAttempts);
        if(message.DeliverBy.HasValue)
            _logger.DebugFormat("Failed to send should retry with DeliverBy: {0}, CurrentTime {1}", message.DeliverBy, DateTime.Now);
        return message.SentAttempts < totalAttempts
               &&
               (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
    }

    private void IncrementAttempt(OutgoingMessage message)
    {
        try
        {
            message.SentAttempts++;
            _store.FailedToSend(message);
        }
        catch (Exception ex)
        {
            _logger.Error("Failed to increment send failure", ex);
        }
    }
}