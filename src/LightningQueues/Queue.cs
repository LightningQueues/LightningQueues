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

/// <summary>
/// Represents a message queue that can send and receive messages across the network.
/// Queue handles the core messaging operations including message sending, receiving,
/// storage, and routing.
/// </summary>
public class Queue : IDisposable
{
    private readonly Sender _sender;
    private readonly Receiver _receiver;
    private readonly Channel<Message> _receivingChannel;
    private readonly CancellationTokenSource _cancelOnDispose;
    private readonly ILogger _logger;
    private Task _sendingTask;
    private Task _receivingTask;

    /// <summary>
    /// Initializes a new instance of the <see cref="Queue"/> class.
    /// </summary>
    /// <param name="receiver">The component responsible for receiving messages from the network.</param>
    /// <param name="sender">The component responsible for sending messages over the network.</param>
    /// <param name="messageStore">The storage system for persisting messages.</param>
    /// <param name="logger">The logger for recording queue operations.</param>
    public Queue(Receiver receiver, Sender sender, IMessageStore messageStore, ILogger logger)
    {
        _receiver = receiver;
        _sender = sender;
        _cancelOnDispose = new CancellationTokenSource();
        Store = messageStore;
        _receivingChannel = Channel.CreateUnbounded<Message>();
        _logger = logger;
    }

    /// <summary>
    /// Gets the network endpoint where this queue is listening for incoming messages.
    /// </summary>
    public IPEndPoint Endpoint => _receiver.Endpoint;

    /// <summary>
    /// Gets an array of all queue names available in this queue instance.
    /// </summary>
    public string[] Queues => Store.GetAllQueues();

    /// <summary>
    /// Gets the message store used by this queue for persistence.
    /// </summary>
    public IMessageStore Store { get; }

    /// <summary>
    /// Creates a new queue with the specified name.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <remarks>
    /// Queue names must be unique within a queue instance. This method creates the 
    /// underlying storage structures needed for the queue.
    /// </remarks>
    public void CreateQueue(string queueName)
    {
        Store.CreateQueue(queueName);
    }

    /// <summary>
    /// Starts the queue's processing operations.
    /// </summary>
    /// <remarks>
    /// This method begins the message receiving and sending operations for the queue.
    /// It must be called after creating the queue and before attempting to send or receive messages.
    /// The method starts background tasks that handle sending and receiving of messages.
    /// </remarks>
    public void Start()
    {
        try
        {
            _sendingTask = StartSendingAsync(_cancelOnDispose.Token);
            _receivingTask = StartReceivingAsync(_cancelOnDispose.Token);
        }
        catch (Exception ex)
        {
            _logger.QueueStartError(ex);
        }
    }

    private async Task StartReceivingAsync(CancellationToken token)
    {
        await _receiver.StartReceivingAsync(_receivingChannel.Writer, token).ConfigureAwait(false);
    }

    private async Task StartSendingAsync(CancellationToken token)
    {
        _logger.QueueStarting();
        var errorPolicy = new SendingErrorPolicy(_logger, Store, _sender.FailedToSend());
        var errorTask = errorPolicy.StartRetries(token);
        
        // Task to handle retry messages by putting them back into outgoing storage
        var retryTask = Task.Run(async () =>
        {
            await foreach (var retryMessage in errorPolicy.Retries.ReadAllAsync(token).ConfigureAwait(false))
            {
                try
                {
                    Store.StoreOutgoing(retryMessage);
                }
                catch (Exception ex)
                {
                    _logger.QueueOutgoingError(ex);
                }
            }
        }, token);
        
        // Start the sending task using storage-based approach
        var sendingTask = Task.Run(async () =>
            await _sender.StartSendingAsync(Store, 50, TimeSpan.FromMilliseconds(200), token).ConfigureAwait(false), 
            token);

        await Task.WhenAll(sendingTask, errorTask.AsTask(), retryTask).ConfigureAwait(false);
    }

    /// <summary>
    /// Receives messages from the specified queue as an asynchronous stream.
    /// </summary>
    /// <param name="queueName">The name of the queue to receive messages from.</param>
    /// <param name="pollIntervalInMilliseconds">The period to rest before checking for new messages if no messages are found.</param>
    /// <param name="cancellationToken">A token to cancel the receive operation.</param>
    /// <returns>
    /// An asynchronous stream of <see cref="MessageContext"/> objects, each containing
    /// a message and its associated queue context for processing.
    /// </returns>
    /// <remarks>
    /// This method returns an IAsyncEnumerable that first yields all persisted messages
    /// from storage, then continuously streams newly arriving messages. The stream continues
    /// until canceled via the cancellation token or when the queue is disposed.
    /// 
    /// Each message is wrapped in a MessageContext that provides operations for
    /// processing the message such as marking it as received, moving it to another queue,
    /// or scheduling it for later processing.
    /// </remarks>
    public async IAsyncEnumerable<MessageContext> Receive(string queueName, int pollIntervalInMilliseconds = 200, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Combine the user's token with our disposal token, creating as few objects as possible
        using var linkedSource = cancellationToken != CancellationToken.None
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancelOnDispose.Token)
            : null;
        var effectiveToken = linkedSource?.Token ?? _cancelOnDispose.Token;

        TimeSpan pollInterval = TimeSpan.FromMilliseconds(pollIntervalInMilliseconds);
        _logger.QueueStartReceiving(queueName);
        
        while (!effectiveToken.IsCancellationRequested)
        {
            var foundNewMessages = false;
            
            // Read messages from storage
            foreach (var message in Store.PersistedIncoming(queueName))
            {
                if (effectiveToken.IsCancellationRequested)
                    yield break;
                
                if (message.Queue.Span.SequenceEqual(queueName.AsSpan()))
                {
                    foundNewMessages = true;
                    yield return new MessageContext(message, this);
                }
            }
            
            // If no new messages found, wait before polling again
            if (!foundNewMessages)
            {
                try
                {
                    await Task.Delay(pollInterval, effectiveToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
            }
        }
    }

    /// <summary>
    /// Moves a message from its current queue to another queue.
    /// </summary>
    /// <param name="queueName">The name of the target queue.</param>
    /// <param name="message">The message to move.</param>
    /// <remarks>
    /// This operation updates the message's queue property and persists the change in storage.
    /// The message becomes immediately available for consumers of the target queue.
    /// </remarks>
    public void MoveToQueue(string queueName, Message message)
    {
        _logger.QueueMoveMessage(message.Id, queueName);
        using var tx = Store.BeginTransaction();
        Store.MoveToQueue(tx, queueName, message);
        tx.Commit();
    }

    /// <summary>
    /// Adds a message directly to a queue for local processing.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    /// <remarks>
    /// Unlike <see cref="Send"/>, this method adds a message directly to a local queue
    /// without sending it over the network. The message is stored for persistence and
    /// made available for immediate processing.
    /// </remarks>
    public void Enqueue(Message message)
    {
        _logger.QueueEnqueue(message.Id, message.QueueString);
        Store.StoreIncoming(message);
    }

    /// <summary>
    /// Schedules a message to be available for processing after a specified delay.
    /// </summary>
    /// <param name="message">The message to delay.</param>
    /// <param name="timeSpan">The time to delay processing of the message.</param>
    /// <remarks>
    /// The message will not be available for receipt until the specified time has elapsed.
    /// This method does not persist the delay information, so if the queue is restarted
    /// before the delay completes, the message may be processed earlier than expected.
    /// </remarks>
    public void ReceiveLater(Message message, TimeSpan timeSpan)
    {
        _logger.QueueReceiveLater(message.Id, timeSpan);
        Task.Delay(timeSpan)
            .ContinueWith(_ =>
            {
                try
                {
                    Store.StoreIncoming(message);
                }
                catch (Exception ex)
                {
                    _logger.QueueErrorReceiveLater(message.Id, timeSpan, ex);
                }
            });
    }

    /// <summary>
    /// Sends multiple messages to their respective destinations.
    /// </summary>
    /// <param name="messages">An array of messages to send.</param>
    /// <remarks>
    /// Each message must have its Destination property set to specify where it should be sent.
    /// The messages are persisted in the outgoing message store before sending to ensure
    /// delivery even if the application crashes or is restarted.
    /// </remarks>
    public void Send(params Message[] messages)
    {
        _logger.QueueSendBatch(messages.Length);
        try
        {
            Store.StoreOutgoing(messages.AsSpan());
        }
        catch (Exception ex)
        {
            _logger.QueueOutgoingError(ex);
        }
    }

    /// <summary>
    /// Sends a single message to its destination.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <remarks>
    /// The message must have its Destination property set to specify where it should be sent.
    /// The message is persisted in the outgoing message store before sending to ensure
    /// delivery even if the application crashes or is restarted.
    /// </remarks>
    public void Send(Message message)
    {
        _logger.QueueSend(message.Id);
        try
        {
            Store.StoreOutgoing(message);
        }
        catch (Exception ex)
        {
            _logger.QueueSendError(message.Id, ex);
        }
    }

    /// <summary>
    /// Schedules a message to be available for processing at a specific time.
    /// </summary>
    /// <param name="message">The message to delay.</param>
    /// <param name="time">The time when the message should become available.</param>
    /// <remarks>
    /// The message will not be available for receipt until the specified time is reached.
    /// This is implemented by calculating the time span between now and the target time.
    /// This method does not persist the delay information, so if the queue is restarted
    /// before the target time, the message may be processed earlier than expected.
    /// </remarks>
    public void ReceiveLater(Message message, DateTimeOffset time)
    {
        ReceiveLater(message, time - DateTimeOffset.Now);
    }

    /// <summary>
    /// Releases all resources used by the queue.
    /// </summary>
    /// <remarks>
    /// This method performs a clean shutdown of the queue by:
    /// 1. Canceling all ongoing operations
    /// 2. Completing message channels to prevent new messages
    /// 3. Waiting for tasks to complete with a timeout
    /// 4. Disposing the sender, receiver, and message store components
    /// 
    /// The method attempts to gracefully shut down all components but includes
    /// timeout logic to prevent hanging indefinitely if a component fails to
    /// respond to cancellation in a timely manner.
    /// </remarks>
    public void Dispose()
    {
        _logger.QueueDispose();

        try
        {
            // First signal cancellation to stop all tasks
            _cancelOnDispose.Cancel();
            
            // Complete the channels to prevent new messages
            _receivingChannel.Writer.TryComplete();
            
            // Give tasks time to respond to cancellation
            try
            {
                // Use a timeout to avoid hanging indefinitely 
                if (_sendingTask != null && _receivingTask != null)
                {
                    var completedTask = Task.WhenAll(_sendingTask, _receivingTask).Wait(TimeSpan.FromSeconds(5));
                    if (!completedTask)
                    {
                        _logger.QueueTasksTimeout();
                    }
                }
            }
            catch (AggregateException ex)
            {
                _logger.QueueTasksDisposeException(ex);
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