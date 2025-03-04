using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net;

/// <summary>
/// Defines the protocol for sending messages over a network connection.
/// </summary>
/// <remarks>
/// The sending protocol is responsible for serializing messages into the wire format
/// and transmitting them to their destination. It handles the outbound communication
/// aspects of the messaging system.
/// 
/// Different protocol implementations can support different versions or formats
/// for backward compatibility or integration with other systems.
/// </remarks>
public interface ISendingProtocol
{
    /// <summary>
    /// Asynchronously sends a batch of messages to a destination.
    /// </summary>
    /// <param name="destination">The URI of the destination queue.</param>
    /// <param name="stream">The network stream to write messages to.</param>
    /// <param name="batch">The list of messages to send.</param>
    /// <param name="token">A token to cancel the operation.</param>
    /// <returns>A task that completes when the messages have been sent.</returns>
    /// <remarks>
    /// This method serializes the messages according to the protocol's format
    /// and writes them to the stream. The destination URI is used to determine
    /// the target endpoint and may influence protocol-specific behavior.
    /// 
    /// The operation can be canceled using the cancellation token, which allows
    /// for graceful shutdown or timeout handling.
    /// </remarks>
    ValueTask SendAsync(Uri destination, Stream stream, List<Message> batch, CancellationToken token);
}