using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using DotNext;
using Microsoft.Extensions.Logging;
using LightningQueues.Net;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;

namespace LightningQueues;

public class Queue : IDisposable
{
    private readonly Sender _sender;
    private readonly Receiver _receiver;
    private readonly Channel<Message> _sendChannel;
    private readonly Channel<Message> _receivingChannel;
    private readonly CancellationTokenSource _cancelOnDispose;
    private readonly ILogger _logger;
    private Task _sendingTask;
    private Task _receivingTask;

    public Queue(Receiver receiver, Sender sender, IMessageStore messageStore, ILogger logger)
    {
        _receiver = receiver;
        _sender = sender;
        _cancelOnDispose = new CancellationTokenSource();
        Store = messageStore;
        _sendChannel = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
        {
            SingleWriter = false, SingleReader = false, AllowSynchronousContinuations = false
        });
        _receivingChannel = Channel.CreateUnbounded<Message>();
        _logger = logger;
    }

    public IPEndPoint Endpoint => _receiver.Endpoint;

    public string[] Queues => Store.GetAllQueues();

    public IMessageStore Store { get; }

    internal ChannelWriter<Message> SendingChannel => _sendChannel.Writer;
    internal ChannelWriter<Message> ReceivingChannel => _receivingChannel.Writer;

    public void CreateQueue(string queueName)
    {
        Store.CreateQueue(queueName);
    }

    public void Start()
    {
        try
        {
            _sendingTask = StartSendingAsync(_cancelOnDispose.Token);
            _receivingTask = StartReceivingAsync(_cancelOnDispose.Token);
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
        var persistedMessages = Store.PersistedOutgoing().ToList();
        var sendingTask = Task.Factory.StartNew(async () =>
        {
            await _sender.StartSendingAsync(_sendChannel.Reader, token).ConfigureAwait(false);
        }, token);
        foreach (var message in persistedMessages)
        {
            await _sendChannel.Writer.WriteAsync(message, token).ConfigureAwait(false);
        }

        var outgoingRetries = errorPolicy.Retries.ReadAllAsync(token);
        await foreach (var message in outgoingRetries)
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

        _logger.QueueStartReceiving(queueName);
        return Store.PersistedIncoming(queueName)
            .Concat(_receivingChannel.Reader.ReadAllAsync(cancellationToken))
            .Where(x => x.Queue == queueName)
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
        _logger.QueueEnqueue(message.Id, message.Queue);
        Store.StoreIncoming(message);
        ReceivingChannel.TryWrite(message);
    }

    public void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        _logger.QueueReceiveLater(message.Id, timeSpan);
        Task.Delay(timeSpan)
            .ContinueWith(_ =>
            {
                if (!ReceivingChannel.TryWrite(message))
                    _logger.QueueErrorReceiveLater(message.Id, timeSpan, null);

            });
    }

    public void Send(params Message[] messages)
    {
        _logger.QueueSendBatch(messages.Length);
        try
        {
            Store.StoreOutgoing(messages);
            foreach (var message in messages)
            {
                if (!_sendChannel.Writer.TryWrite(message))
                    throw new Exception("Failed to send message");//throw better error
            }
        }
        catch (Exception ex)
        {
            if(_logger.IsEnabled(LogLevel.Error))
                _logger.LogError(ex, "Error sending queue outgoing messages");
        }
    }
    
    public void Send(Message message)
    {
        _logger.QueueSend(message.Id);
        try
        {
            Store.StoreOutgoing(message);
            if (!_sendChannel.Writer.TryWrite(message))
                throw new Exception("Failed to send message");//throw better error
        }
        catch (Exception ex)
        {
            _logger.QueueSendError(message.Id, ex);
        }
    }

    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        ReceiveLater(message, time - DateTimeOffset.Now);
    }

    public void Dispose()
    {
        _logger.QueueDispose();

        try
        {
            // First signal cancellation to stop all tasks
            _cancelOnDispose.Cancel();
            
            // Complete the channels to prevent new messages
            _receivingChannel.Writer.TryComplete();
            _sendChannel.Writer.TryComplete();
            
            // Give tasks time to respond to cancellation
            try
            {
                // Use a timeout to avoid hanging indefinitely 
                if (_sendingTask != null && _receivingTask != null)
                {
                    var completedTask = Task.WhenAll(_sendingTask, _receivingTask).Wait(TimeSpan.FromSeconds(5));
                    if (!completedTask && _logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning("Tasks did not complete within timeout during disposal");
                    }
                }
            }
            catch (AggregateException ex)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug(ex, "Exception waiting for tasks to complete during disposal");
            }
            
            // Now dispose components in correct order
            // Dispose sender and receiver first as they might be using the store
            _sender?.Dispose();
            _receiver?.Dispose();
            
            // Finally dispose the store and cancellation token
            Store?.Dispose();
            _cancelOnDispose?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.QueueDisposeError(ex);
        }
        GC.SuppressFinalize(this);
    }
}