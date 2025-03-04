using System;
using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security;

/// <summary>
/// Defines the contract for applying security mechanisms to network streams.
/// </summary>
/// <remarks>
/// The stream security interface allows different security mechanisms (such as TLS)
/// to be applied to the underlying transport connections used for message exchange.
/// Implementations can provide encryption, authentication, and integrity checking
/// for messages in transit.
/// </remarks>
public interface IStreamSecurity
{
    /// <summary>
    /// Applies security mechanisms to a network stream.
    /// </summary>
    /// <param name="endpoint">The endpoint URI being connected to or received from.</param>
    /// <param name="stream">The raw network stream to secure.</param>
    /// <returns>A task that completes with the secured stream.</returns>
    /// <remarks>
    /// This method wraps the provided stream with security mechanisms (like encryption)
    /// and returns the wrapped stream. The endpoint parameter can be used to determine
    /// endpoint-specific security settings.
    /// </remarks>
    ValueTask<Stream> Apply(Uri endpoint, Stream stream);
}