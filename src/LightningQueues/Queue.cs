using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Logging;
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
            var sending = StartSendingAsync();
            var receiving = StartReceivingAsync();
            await Task.WhenAll(sending, receiving);
        }
        catch (Exception ex)
        {
            _logger.Error("Error starting queue", ex);
        }
    }

    private async Task StartReceivingAsync()
    {
        await _receiver.StartReceivingAsync(_receivingChannel.Writer, _cancelOnDispose.Token);
    }

    private async Task StartSendingAsync()
    {
        _logger.Debug("Starting LightningQueues");
        var errorPolicy = new SendingErrorPolicy(_logger, Store, _sender.FailedToSend());
        var errorTask = errorPolicy.StartRetries(_cancelOnDispose.Token);
        var persistedMessages = Store.PersistedOutgoingMessages();
        var sendingTask = Task.Factory.StartNew(async () =>
        {
            await _sender.StartSendingAsync(_sendChannel.Reader, _cancelOnDispose.Token);
        }, _cancelOnDispose.Token);
        foreach (var message in persistedMessages)
        {
            await _sendChannel.Writer.WriteAsync(message);
        }

        var outgoingRetries = errorPolicy.Retries.ReadAllAsync(_cancelOnDispose.Token);
        await foreach (var message in outgoingRetries)
        {
            await _sendChannel.Writer.WriteAsync(message, _cancelOnDispose.Token);
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
        
        _logger.DebugFormat("Starting to receive for queue {0}", queueName);
        return Store.PersistedMessages(queueName).ToAsyncEnumerable(cancellationToken)
            .Concat(_receivingChannel.Reader.ReadAllAsync(cancellationToken)
                    .Where(x => x.Queue == queueName))
            .Select(x => new MessageContext(x, this));
    }

    public void MoveToQueue(string queueName, Message message)
    {
        _logger.DebugFormat("Moving message {0} to {1}", message.Id.MessageIdentifier, queueName);
        var tx = Store.BeginTransaction();
        Store.MoveToQueue(tx, queueName, message);
        tx.Commit();
        message.Queue = queueName;
        ReceivingChannel.TryWrite(message);
    }

    public void Enqueue(Message message)
    {
        _logger.DebugFormat("Enqueueing message {0} to queue {1}", message.Id.MessageIdentifier, message.Queue);
        Store.StoreIncomingMessages(message);
        ReceivingChannel.TryWrite(message);
    }

    public async void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        _logger.DebugFormat("Delaying message {0} until {1}", message.Id.MessageIdentifier, timeSpan);
        try
        {
            await Task.Delay(timeSpan);
            await ReceivingChannel.WriteAsync(message, _cancelOnDispose.Token);
        }
        catch (Exception ex)
        {
            _logger.Error("Error with receiving later", ex);
        }
    }

    public async void Send(params OutgoingMessage[] messages)
    {
        _logger.DebugFormat("Sending {0} messages", messages.Length);
        var tx = Store.BeginTransaction();
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

    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        ReceiveLater(message, time - DateTimeOffset.Now);
    }

    public void Dispose()
    {
        _logger.Info("Disposing queue");
        Store.Dispose();
        try
        {
            _cancelOnDispose.Cancel();
            _sender.Dispose();
            _receiver.Dispose();
            _receivingChannel.Writer.Complete();
            _sendChannel.Writer.Complete();
        }
        catch (Exception e)
        {
            _logger.Error("Failed when shutting down queue", e);
        }
        GC.SuppressFinalize(this);
    }
}