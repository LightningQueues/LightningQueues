using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net;

public class SendingErrorPolicy
{
    private readonly ILogger _logger;
    private readonly IMessageStore _store;
    private readonly Channel<OutgoingMessageFailure> _failedToConnect;
    private readonly Channel<Message> _retries;

    public SendingErrorPolicy(ILogger logger, IMessageStore store, Channel<OutgoingMessageFailure> failedToConnect)
    {
        _logger = logger;
        _store = store;
        _failedToConnect = failedToConnect;
        _retries = Channel.CreateUnbounded<Message>();
    }

    public ChannelReader<Message> Retries => _retries.Reader;

    public async ValueTask StartRetries(CancellationToken cancellationToken)
    {
        await foreach (var messageFailure in _failedToConnect.Reader.ReadAllAsync(cancellationToken))
        {
            foreach (var message in messageFailure.Messages)
            {
                IncrementAttempt(message);
                if (!ShouldRetry(message)) 
                    continue;
                await Task.Delay(TimeSpan.FromSeconds(message.SentAttempts * message.SentAttempts), cancellationToken);
                await _retries.Writer.WriteAsync(message, cancellationToken);
            }
        }
    }

    public bool ShouldRetry(Message message)
    {
        var totalAttempts = message.MaxAttempts ?? 100;
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Failed to send should retry with on: {AttemptCount}, out of {TotalAttempts}", message.SentAttempts, totalAttempts);
        if(message.DeliverBy.HasValue && _logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Failed to send should retry with: {DeliverBy}, due to {CurrentTime}", message.DeliverBy, DateTime.Now);
        return message.SentAttempts < totalAttempts
               &&
               (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
    }

    private void IncrementAttempt(Message message)
    {
        try
        {
            message.SentAttempts++;
            _store.FailedToSend(message);
        }
        catch (Exception ex)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(ex, "Failed to increment send failure");
        }
    }
}