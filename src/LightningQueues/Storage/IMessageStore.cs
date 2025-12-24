using System;
using System.Collections.Generic;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Storage;

/// <summary>
/// Defines the contract for message persistence in LightningQueues.
/// </summary>
/// <remarks>
/// The message store is responsible for persisting messages to ensure they
/// are not lost in case of application restarts or crashes. It handles both
/// incoming messages (received from other queues) and outgoing messages
/// (to be sent to other queues).
/// </remarks>
public interface IMessageStore : IDisposable
{
    /// <summary>
    /// Begins a new transaction for performing storage operations.
    /// </summary>
    /// <returns>A new transaction object that can be used for storage operations.</returns>
    /// <remarks>
    /// Transactions ensure atomic operations on the message store and can be
    /// committed or aborted.
    /// </remarks>
    LmdbTransaction BeginTransaction();
    
    /// <summary>
    /// Creates a new queue with the specified name.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <remarks>
    /// This method creates the underlying storage structures needed for a queue.
    /// Queue names must be unique within a store.
    /// </remarks>
    void CreateQueue(string queueName);
    
    /// <summary>
    /// Stores incoming messages in the message store.
    /// </summary>
    /// <param name="messages">The messages to store.</param>
    /// <remarks>
    /// This method persists incoming messages that have been received from other queues
    /// or added directly to a local queue.
    /// </remarks>
    void StoreIncoming(params IEnumerable<Message> messages);
    
    /// <summary>
    /// Stores incoming messages using an existing transaction.
    /// </summary>
    /// <param name="transaction">The transaction to use for the storage operation.</param>
    /// <param name="messages">The messages to store.</param>
    /// <remarks>
    /// This overload allows multiple storage operations to be performed
    /// within a single transaction for atomic behavior.
    /// </remarks>
    void StoreIncoming(LmdbTransaction transaction, params IEnumerable<Message> messages);

    /// <summary>
    /// Stores incoming messages using raw wire-format bytes (zero-copy path).
    /// </summary>
    /// <param name="messages">Pre-parsed message info from WireFormatSplitter.</param>
    /// <param name="count">Number of messages in the array to store.</param>
    /// <param name="serializer">Serializer for fallback deserialization if needed.</param>
    /// <remarks>
    /// This method enables zero-copy storage by accepting pre-parsed wire format data.
    /// Implementations can store raw bytes directly without re-serialization.
    /// The default implementation falls back to deserializing and using StoreIncoming.
    /// </remarks>
    void StoreRawIncoming(RawMessageInfo[] messages, int count, IMessageSerializer serializer)
    {
        // Default implementation: deserialize and use regular storage path
        var deserializedMessages = new List<Message>(count);
        for (var i = 0; i < count; i++)
        {
            var msg = serializer.ToMessage(messages[i].FullMessage.Span);
            deserializedMessages.Add(msg);
        }
        StoreIncoming(deserializedMessages);
    }

    /// <summary>
    /// Deletes incoming messages from the message store.
    /// </summary>
    /// <param name="messages">The messages to delete.</param>
    /// <remarks>
    /// This method is typically used to clean up messages that are no longer needed.
    /// </remarks>
    void DeleteIncoming(params IEnumerable<Message> messages);
    
    /// <summary>
    /// Retrieves all persisted incoming messages for a specified queue.
    /// </summary>
    /// <param name="queueName">The name of the queue to retrieve messages from.</param>
    /// <returns>An enumerable collection of messages from the specified queue.</returns>
    /// <remarks>
    /// This method is typically used during queue initialization to load previously
    /// received messages that have not yet been processed.
    /// </remarks>
    IEnumerable<Message> PersistedIncoming(string queueName);
    
    /// <summary>
    /// Retrieves all persisted outgoing messages.
    /// </summary>
    /// <returns>An enumerable collection of outgoing messages.</returns>
    /// <remarks>
    /// This method is typically used during queue initialization to reload
    /// messages that were queued for sending but had not been sent when
    /// the application was previously shut down.
    /// </remarks>
    IEnumerable<Message> PersistedOutgoing();

    /// <summary>
    /// Retrieves all persisted outgoing messages as raw wire-format bytes.
    /// </summary>
    /// <returns>An enumerable collection of raw outgoing messages with routing info extracted.</returns>
    /// <remarks>
    /// This method enables zero-copy sending by returning raw bytes with only routing
    /// information (destination, queue) extracted via WireFormatReader.
    /// The default implementation falls back to PersistedOutgoing() with serialization.
    /// </remarks>
    IEnumerable<RawOutgoingMessage> PersistedOutgoingRaw()
    {
        // Default implementation: fall back to regular enumeration with serialization
        // This is overridden in LmdbMessageStore with an optimized implementation
        throw new NotSupportedException("Raw outgoing enumeration requires LmdbMessageStore");
    }

    /// <summary>
    /// Moves a message to a different queue.
    /// </summary>
    /// <param name="transaction">The transaction to use for the operation.</param>
    /// <param name="queueName">The name of the target queue.</param>
    /// <param name="message">The message to move.</param>
    /// <remarks>
    /// This method changes the queue association of a message, making it
    /// available for consumers of the target queue.
    /// </remarks>
    void MoveToQueue(LmdbTransaction transaction, string queueName, Message message);
    
    /// <summary>
    /// Marks a message as successfully received and processed.
    /// </summary>
    /// <param name="transaction">The transaction to use for the operation.</param>
    /// <param name="message">The message that has been processed.</param>
    /// <remarks>
    /// This method removes the message from the incoming messages store
    /// after successful processing.
    /// </remarks>
    void SuccessfullyReceived(LmdbTransaction transaction, Message message);
    
    /// <summary>
    /// Stores an outgoing message using an existing transaction.
    /// </summary>
    /// <param name="tx">The transaction to use for the operation.</param>
    /// <param name="message">The message to store.</param>
    /// <remarks>
    /// This method persists a message that is to be sent to another queue.
    /// </remarks>
    void StoreOutgoing(LmdbTransaction tx, Message message);
    
    /// <summary>
    /// Stores a single outgoing message.
    /// </summary>
    /// <param name="message">The message to store.</param>
    /// <remarks>
    /// This method persists a message that is to be sent to another queue.
    /// This overload is optimized for single message operations.
    /// </remarks>
    void StoreOutgoing(Message message);

    /// <summary>
    /// Stores multiple outgoing messages.
    /// </summary>
    /// <param name="messages">The messages to store.</param>
    /// <remarks>
    /// This method persists messages that are to be sent to other queues.
    /// </remarks>
    void StoreOutgoing(params IEnumerable<Message> messages);

    /// <summary>
    /// Stores multiple outgoing messages using a span.
    /// </summary>
    /// <param name="messages">The span of messages to store.</param>
    /// <remarks>
    /// This method persists messages that are to be sent to other queues.
    /// Uses ReadOnlySpan for better performance by avoiding array allocations.
    /// </remarks>
    void StoreOutgoing(ReadOnlySpan<Message> messages);
    
    /// <summary>
    /// Handles messages that failed to send.
    /// </summary>
    /// <param name="shouldRemove">Whether the messages should be removed from the outgoing store.</param>
    /// <param name="message">The messages that failed to send.</param>
    /// <remarks>
    /// This method updates the state of messages that could not be sent.
    /// If shouldRemove is true, the messages are removed from the outgoing store;
    /// otherwise, they are marked for retry according to the queue's retry policy.
    /// </remarks>
    void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> message);
    
    /// <summary>
    /// Marks messages as successfully sent.
    /// </summary>
    /// <param name="messages">The messages that have been sent.</param>
    /// <remarks>
    /// This method removes messages from the outgoing store after
    /// they have been successfully transmitted to their destinations.
    /// </remarks>
    void SuccessfullySent(params IEnumerable<Message> messages);

    /// <summary>
    /// Marks messages as successfully sent using raw MessageIds (zero-copy path).
    /// </summary>
    /// <param name="messageIds">The raw 16-byte MessageIds to remove from outgoing storage.</param>
    /// <remarks>
    /// This method removes messages from the outgoing store by their raw MessageId bytes.
    /// It's more efficient than SuccessfullySent when used with PersistedOutgoingRaw()
    /// as it avoids creating Message objects.
    /// </remarks>
    void SuccessfullySentByIds(IEnumerable<ReadOnlyMemory<byte>> messageIds)
    {
        // Default implementation: not supported
        throw new NotSupportedException("Raw MessageId deletion requires LmdbMessageStore");
    }
    
    /// <summary>
    /// Retrieves a specific message by its ID from a specified queue.
    /// </summary>
    /// <param name="queueName">The name of the queue to search.</param>
    /// <param name="messageId">The ID of the message to retrieve.</param>
    /// <returns>The requested message, or null if not found.</returns>
    /// <remarks>
    /// This method is typically used for administrative purposes or
    /// for implementing message tracking features.
    /// </remarks>
    Message? GetMessage(string queueName, MessageId messageId);
    
    /// <summary>
    /// Gets an array of all queue names in the store.
    /// </summary>
    /// <returns>An array of queue names.</returns>
    /// <remarks>
    /// This method is typically used for administrative purposes or
    /// for displaying available queues to users.
    /// </remarks>
    string[] GetAllQueues();
    
    /// <summary>
    /// Clears all stored messages and queue definitions.
    /// </summary>
    /// <remarks>
    /// This method should be used with caution as it permanently deletes
    /// all messages and queue definitions from the store.
    /// </remarks>
    void ClearAllStorage();
}