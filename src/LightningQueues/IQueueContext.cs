using System;

namespace LightningQueues;

/// <summary>
/// Defines the operations available for processing a message in a queue.
/// </summary>
/// <remarks>
/// IQueueContext provides methods for handling messages during processing,
/// such as committing changes, sending responses, deferring processing,
/// or moving messages to different queues.
/// </remarks>
public interface IQueueContext
{
    /// <summary>
    /// Commits any pending changes to the message.
    /// </summary>
    /// <remarks>
    /// This method persists any modifications made to the message during processing.
    /// It should be called after successful message processing to ensure changes are saved.
    /// </remarks>
    void CommitChanges();
    
    /// <summary>
    /// Sends a message to its specified destination.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <remarks>
    /// This method is typically used to send a response or follow-up message
    /// after processing the current message.
    /// </remarks>
    void Send(Message message);
    
    /// <summary>
    /// Schedules the current message to be processed again after a specified delay.
    /// </summary>
    /// <param name="timeSpan">The time to wait before making the message available again.</param>
    /// <remarks>
    /// This is useful for implementing retry logic or delayed processing.
    /// </remarks>
    void ReceiveLater(TimeSpan timeSpan);
    
    /// <summary>
    /// Schedules the current message to be processed again at a specific time.
    /// </summary>
    /// <param name="time">The time at which the message should be made available again.</param>
    /// <remarks>
    /// This is useful for scheduling message processing at a specific point in time.
    /// </remarks>
    void ReceiveLater(DateTimeOffset time);
    
    /// <summary>
    /// Marks the current message as successfully received and processed.
    /// </summary>
    /// <remarks>
    /// This method removes the message from the queue after successful processing.
    /// It should be called when message processing is complete and the message
    /// no longer needs to be kept in the queue.
    /// </remarks>
    void SuccessfullyReceived();
    
    /// <summary>
    /// Moves the current message to a different queue.
    /// </summary>
    /// <param name="queueName">The name of the destination queue.</param>
    /// <remarks>
    /// This is useful for implementing workflow stages or error handling,
    /// where messages need to be routed to different queues based on processing results.
    /// </remarks>
    void MoveTo(string queueName);
    
    /// <summary>
    /// Adds a new message to the current queue.
    /// </summary>
    /// <param name="message">The message to enqueue.</param>
    /// <remarks>
    /// This method is used to add a new message directly to the queue
    /// for local processing, without sending it over the network.
    /// </remarks>
    void Enqueue(Message message);
}