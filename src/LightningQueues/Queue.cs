using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Net;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;

namespace LightningQueues;

public class Queue : IDisposable
{
    private readonly Sender _sender;
    private readonly Receiver _receiver;
    private readonly Channel<OutgoingMessage> _sendChannel;
    private readonly Channel<Message> _receivingChannel;
    private readonly CancellationTokenSource _cancelOnDispose;
    private readonly ILogger _logger;

    public Queue(Receiver receiver, Sender sender, IMessageStore messageStore, ILogger logger)
    {
        _receiver = receiver;
        _sender = sender;
        _cancelOnDispose = new CancellationTokenSource();
        Store = messageStore;
        _sendChannel = Channel.CreateUnbounded<OutgoingMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false, SingleReader = false, AllowSynchronousContinuations = false
        });
        _receivingChannel = Channel.CreateUnbounded<Message>();
        _logger = logger;
    }

    public IPEndPoint Endpoint => _receiver.Endpoint;

    public string[] Queues => Store.GetAllQueues();

    public IMessageStore Store { get; }

    internal ChannelWriter<OutgoingMessage> SendingChannel => _sendChannel.Writer;
    internal ChannelWriter<Message> ReceivingChannel => _receivingChannel.Writer;

    public void CreateQueue(string queueName)
    {
        Store.CreateQueue(queueName);
    }

    public async void Start()
    {
        try
        {
            var sending = StartSendingAsync(_cancelOnDispose.Token);
            var receiving = StartReceivingAsync(_cancelOnDispose.Token);
            await Task.WhenAll(sending, receiving);
        }
        catch (Exception ex)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(ex, "Error starting queue");
        }
    }

    private async Task StartReceivingAsync(CancellationToken token)
    {
        await _receiver.StartReceivingAsync(_receivingChannel.Writer, token);
    }

    private async Task StartSendingAsync(CancellationToken token)
    {
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Starting LightningQueues");
        var errorPolicy = new SendingErrorPolicy(_logger, Store, _sender.FailedToSend());
        var errorTask = errorPolicy.StartRetries(token);
        var persistedMessages = Store.PersistedOutgoingMessages();
        var sendingTask = Task.Factory.StartNew(async () =>
        {
            await _sender.StartSendingAsync(_sendChannel.Reader, token).ConfigureAwait(false);
        }, token);
        foreach (var message in persistedMessages)
        {
            await _sendChannel.Writer.WriteAsync(message, token).ConfigureAwait(false);
        }

        var outgoingRetries = errorPolicy.Retries.ReadAllAsync(token);
        await foreach (var message in outgoingRetries.WithCancellation(token))
        {
            await _sendChannel.Writer.WriteAsync(message, token);
        }

        await Task.WhenAll(sendingTask, errorTask.AsTask());
    }

    public IAsyncEnumerable<MessageContext> Receive(string queueName, CancellationToken cancellationToken = default)
    {
        if (cancellationToken != default)
            cancellationToken = CancellationTokenSource
                .CreateLinkedTokenSource(cancellationToken, _cancelOnDispose.Token).Token;
        else
            cancellationToken = _cancelOnDispose.Token;
        
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Starting to receive for queue {QueueName}", queueName);
        return Store.PersistedMessages(queueName).ToAsyncEnumerable(cancellationToken)
            .Concat(_receivingChannel.Reader.ReadAllAsync(cancellationToken)
                    .Where(x => x.Queue == queueName))
            .Select(x => new MessageContext(x, this));
    }

    public void MoveToQueue(string queueName, Message message)
    {
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Moving message {MessageIdentifier} to {QueueName}", message.Id.MessageIdentifier, queueName);
        using var tx = Store.BeginTransaction();
        Store.MoveToQueue(tx, queueName, message);
        tx.Commit();
        message.Queue = queueName;
        ReceivingChannel.TryWrite(message);
    }

    public void Enqueue(Message message)
    {
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Enqueueing message {MessageIdentifier} to {QueueName}", message.Id.MessageIdentifier, message.Queue);
        Store.StoreIncomingMessages(message);
        ReceivingChannel.TryWrite(message);
    }

    public async void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        if(_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Delaying message {MessageIdentifier} until {DelayTime}", message.Id.MessageIdentifier, message.Queue);
        try
        {
            await Task.Delay(timeSpan);
            await ReceivingChannel.WriteAsync(message, _cancelOnDispose.Token);
        }
        catch (Exception ex)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(ex, "Error with receiving later");
        }
    }

    public async void Send(params OutgoingMessage[] messages)
    {
        if (_logger.IsEnabled(LogLevel.Error))
            _logger.LogDebug("Sending {MessageCount} messages", messages.Length);
        try
        {
            using var tx = Store.BeginTransaction();
            foreach (var message in messages)
            {
                Store.StoreOutgoing(tx, message);
            }
            tx.Commit();
            foreach (var message in messages)
            {
                await _sendChannel.Writer.WriteAsync(message, _cancelOnDispose.Token);
            }
        }
        catch (Exception ex)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(ex, "Error sending queue outgoing messages");
        }
    }

    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        ReceiveLater(message, time - DateTimeOffset.Now);
    }

    public void Dispose()
    {
        if(_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Disposing queue");
        
        _cancelOnDispose.Cancel();
        _cancelOnDispose.Dispose();
        try
        {
            _sender.Dispose();
            _receiver.Dispose();
            _receivingChannel.Writer.TryComplete();
            _sendChannel.Writer.TryComplete();
            Store.Dispose();
        }
        catch (Exception e)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(e, "Failed when shutting down queue");
        }
        GC.SuppressFinalize(this);
    }
}