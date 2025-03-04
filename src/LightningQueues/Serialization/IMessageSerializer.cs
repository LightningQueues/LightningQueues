using System;
using System.Buffers;
using System.Collections.Generic;

namespace LightningQueues.Serialization;

/// <summary>
/// Defines the contract for serializing and deserializing messages.
/// </summary>
/// <remarks>
/// The message serializer is responsible for converting message objects to binary format
/// for network transmission and storage, and back again for processing. Implementations
/// can use different serialization formats as long as they adhere to this interface.
/// </remarks>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a list of messages to a memory buffer.
    /// </summary>
    /// <param name="messages">The list of messages to serialize.</param>
    /// <returns>A read-only memory containing the serialized binary data.</returns>
    /// <remarks>
    /// This method is typically used for batch serialization when sending multiple 
    /// messages over the network.
    /// </remarks>
    ReadOnlyMemory<byte> ToMemory(List<Message> messages);
    
    /// <summary>
    /// Deserializes messages from a binary buffer.
    /// </summary>
    /// <param name="buffer">The binary buffer containing serialized messages.</param>
    /// <returns>A list of deserialized message objects.</returns>
    /// <remarks>
    /// This method is typically used when receiving batches of messages from the network.
    /// </remarks>
    IList<Message> ReadMessages(ReadOnlySequence<byte> buffer);
    
    /// <summary>
    /// Serializes a single message to a read-only span.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <returns>A read-only span containing the serialized binary data.</returns>
    /// <remarks>
    /// This method is typically used for single message serialization and provides
    /// a more memory-efficient alternative to ToMemory for single messages.
    /// </remarks>
    ReadOnlySpan<byte> AsSpan(Message message);
    
    /// <summary>
    /// Deserializes a single message from a binary buffer.
    /// </summary>
    /// <param name="buffer">The binary buffer containing a serialized message.</param>
    /// <returns>The deserialized message object.</returns>
    /// <remarks>
    /// This method is typically used when receiving a single message or when retrieving
    /// individual messages from storage.
    /// </remarks>
    Message ToMessage(ReadOnlySpan<byte> buffer);
}