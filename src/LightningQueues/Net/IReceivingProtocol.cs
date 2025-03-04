using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net;

/// <summary>
/// Defines the protocol for receiving messages over a network connection.
/// </summary>
/// <remarks>
/// The receiving protocol is responsible for understanding the wire format
/// of incoming messages, deserializing them, and making them available for
/// processing by the queue.
/// 
/// Different protocol implementations can support different versions or formats
/// for backward compatibility or integration with other systems.
/// </remarks>
public interface IReceivingProtocol
{
    /// <summary>
    /// Asynchronously receives messages from a network stream.
    /// </summary>
    /// <param name="stream">The network stream to read messages from.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that completes with a list of received messages.</returns>
    /// <remarks>
    /// This method reads data from the stream according to the protocol's format,
    /// deserializes the messages, and returns them for processing.
    /// 
    /// The operation can be canceled using the cancellation token, which allows
    /// for graceful shutdown or timeout handling.
    /// </remarks>
    Task<IList<Message>> ReceiveMessagesAsync(Stream stream, CancellationToken cancellationToken);
}