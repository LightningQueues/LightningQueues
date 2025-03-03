using System;
using System.Collections.Generic;
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
            IncrementSentAttempt(messageFailure.Messages);
            IncrementAttemptAndStoreForRecovery(!messageFailure.ShouldRetry, messageFailure.Messages);
            await HandleMessageRetries(messageFailure.ShouldRetry, cancellationToken, messageFailure.Messages);
        }
    }

    private async Task HandleMessageRetries(bool shouldRetry, CancellationToken cancellationToken, params IEnumerable<Message> messages)
    {
        foreach (var message in messages)
        {
            if (!ShouldRetry(message, shouldRetry)) 
                continue;
            await Task.Delay(TimeSpan.FromSeconds(message.SentAttempts * message.SentAttempts), cancellationToken);
            await _retries.Writer.WriteAsync(message, cancellationToken);
        }
    }

    public bool ShouldRetry(Message message, bool shouldRetryOverride = true)
    {
        if (!shouldRetryOverride)
            return false;
        var totalAttempts = message.MaxAttempts ?? 100;
        _logger.PolicyShouldRetryAttempts(message.SentAttempts, totalAttempts);
        if(message.DeliverBy.HasValue)
            _logger.PolicyShouldRetryTiming(message.DeliverBy, DateTime.Now);
        return message.SentAttempts < totalAttempts
               &&
               (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
    }

    private void IncrementAttemptAndStoreForRecovery(bool shouldRemove, params IEnumerable<Message> messages)
    {
        try
        {

            _store.FailedToSend(shouldRemove, messages);
        }
        catch (Exception ex)
        {
            _logger.PolicyIncrementFailureError(ex);
        }
    }

    private static void IncrementSentAttempt(IEnumerable<Message> messages)
    {
        foreach (var message in messages)
        {
            message.SentAttempts++;
        }
    }
}