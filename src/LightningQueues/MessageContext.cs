namespace LightningQueues;

/// <summary>
/// Represents a context for processing a message within a queue.
/// </summary>
/// <remarks>
/// MessageContext encapsulates a message along with its processing context,
/// providing access to both the message content and operations that can be
/// performed on the message during processing.
/// </remarks>
public class MessageContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContext"/> class.
    /// </summary>
    /// <param name="message">The message being processed.</param>
    /// <param name="queue">The queue containing the message.</param>
    internal MessageContext(Message message, Queue queue)
    {
        Message = message;
        QueueContext = new QueueContext(queue, message);
    }

    /// <summary>
    /// Gets the message being processed.
    /// </summary>
    /// <remarks>
    /// This property provides access to the message content, headers, and metadata.
    /// </remarks>
    public Message Message { get; }
    
    /// <summary>
    /// Gets the queue context for the message.
    /// </summary>
    /// <remarks>
    /// The queue context provides operations for processing the message,
    /// such as marking it as received, moving it to another queue,
    /// or scheduling it for later processing.
    /// </remarks>
    public IQueueContext QueueContext { get; }
}