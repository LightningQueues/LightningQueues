using System;
using System.Net;
using LightningDB;
using LightningQueues.Net;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Net.Tcp;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Microsoft.Extensions.Logging;

namespace LightningQueues;

/// <summary>
/// Provides a fluent API for configuring and building queue instances.
/// </summary>
/// <remarks>
/// QueueConfiguration allows you to configure various aspects of a queue such as
/// message storage, network endpoint, protocols, security, and serialization.
/// It uses a builder pattern to create properly configured Queue instances.
/// </remarks>
public class QueueConfiguration
{
    #region NoLogging
    /// <summary>
    /// Provides a no-op logger implementation that discards all log messages.
    /// </summary>
    /// <remarks>
    /// This is used as the default logger when none is explicitly provided.
    /// </remarks>
    private class NoLoggingLogger : ILogger
    {
        /// <summary>
        /// Ignores the log message and does nothing.
        /// </summary>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
        }

        /// <summary>
        /// Always returns false to indicate that no logging is enabled.
        /// </summary>
        public bool IsEnabled(LogLevel logLevel)
        {
            return false;
        }

        /// <summary>
        /// Returns null as scopes are not supported.
        /// </summary>
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }
    }
    #endregion //NoLogging

    private IStreamSecurity? _sendingSecurity;
    private IStreamSecurity? _receivingSecurity;
    private Func<IMessageStore>? _store;
    private IPEndPoint? _endpoint;
    private IReceivingProtocol? _receivingProtocol;
    private ISendingProtocol? _sendingProtocol;
    private IMessageSerializer? _serializer;
    private ILogger? _logger;
    private TimeSpan _timeoutBatchAfter;
    
    /// <summary>
    /// Gets the message serializer used for converting messages to and from binary format.
    /// </summary>
    public IMessageSerializer? Serializer => _serializer;

    /// <summary>
    /// Configures the message storage mechanism for the queue.
    /// </summary>
    /// <param name="store">A factory function that creates the message store instance.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// The message store is responsible for persisting messages to ensure they
    /// are not lost in case of application restarts or crashes.
    /// </remarks>
    public QueueConfiguration StoreMessagesWith(Func<IMessageStore> store)
    {
        _store = store;
        return this;
    }

    /// <summary>
    /// Sets the timeout period for network batches of messages.
    /// </summary>
    /// <param name="timeSpan">The timeout period.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// This setting determines how long the queue will wait to batch multiple messages
    /// together before sending them over the network, which can improve performance.
    /// </remarks>
    public QueueConfiguration TimeoutNetworkBatchAfter(TimeSpan timeSpan)
    {
        _timeoutBatchAfter = timeSpan;
        return this;
    }

    /// <summary>
    /// Configures the network endpoint where the queue will listen for incoming messages.
    /// </summary>
    /// <param name="endpoint">The IP endpoint (address and port) to listen on.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// This endpoint will be used by other queue instances to send messages to this queue.
    /// </remarks>
    public QueueConfiguration ReceiveMessagesAt(IPEndPoint endpoint)
    {
        _endpoint = endpoint;
        return this;
    }

    /// <summary>
    /// Configures the protocols used for sending and receiving messages.
    /// </summary>
    /// <param name="receivingProtocol">The protocol for receiving messages.</param>
    /// <param name="sendingProtocol">The protocol for sending messages.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// Protocols define the wire format and communication patterns used for message exchange.
    /// Custom protocols can be provided to support different versions or formats.
    /// </remarks>
    public QueueConfiguration CommunicateWithProtocol(IReceivingProtocol receivingProtocol, ISendingProtocol sendingProtocol)
    {
        _receivingProtocol = receivingProtocol;
        _sendingProtocol = sendingProtocol;
        return this;
    }

    /// <summary>
    /// Configures the queue to use an automatically assigned endpoint on the local machine.
    /// </summary>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// This is useful for development or testing scenarios where the specific port
    /// doesn't matter, as long as it's available.
    /// </remarks>
    public QueueConfiguration AutomaticEndpoint()
    {
        return ReceiveMessagesAt(new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort()));
    }

    /// <summary>
    /// Configures the serializer used to convert messages to and from binary format.
    /// </summary>
    /// <param name="serializer">The message serializer to use.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// The serializer handles conversion of message objects to binary format for
    /// network transmission and storage, and back again for processing.
    /// </remarks>
    public QueueConfiguration SerializeWith(IMessageSerializer serializer)
    {
        _serializer = serializer;
        return this;
    }

    /// <summary>
    /// Configures the logger used for queue operations.
    /// </summary>
    /// <param name="logger">The logger instance to use.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// The logger records operational events, errors, and debugging information
    /// for queue operations.
    /// </remarks>
    public QueueConfiguration LogWith(ILogger logger)
    {
        _logger = logger;
        return this;
    }

    /// <summary>
    /// Configures the security mechanisms for sending and receiving messages.
    /// </summary>
    /// <param name="sending">The security configuration for outgoing messages.</param>
    /// <param name="receiving">The security configuration for incoming messages.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// Security mechanisms can provide encryption, authentication, and integrity
    /// checking for messages in transit.
    /// </remarks>
    public QueueConfiguration SecureTransportWith(IStreamSecurity sending, IStreamSecurity receiving)
    {
        _receivingSecurity = receiving;
        _sendingSecurity = sending;
        return this;
    }

    /// <summary>
    /// Configures the queue with sensible default settings.
    /// </summary>
    /// <param name="path">Optional path for message storage. If provided, LMDB storage will be used.</param>
    /// <returns>The configuration object for method chaining.</returns>
    /// <remarks>
    /// This method provides a quick way to get started with reasonable defaults:
    /// - Standard message serializer
    /// - No logging
    /// - No transport security
    /// - 5-second network batch timeout
    /// - Automatic endpoint selection
    /// - LMDB storage if a path is provided
    /// </remarks>
    public QueueConfiguration WithDefaults(string? path = null)
    {
        SerializeWith(new MessageSerializer());
        LogWith(new NoLoggingLogger());
        SecureTransportWith(new NoSecurity(), new NoSecurity());
        TimeoutNetworkBatchAfter(TimeSpan.FromSeconds(5));
        AutomaticEndpoint();
        if(path != null)
            this.StoreWithLmdb(path, new EnvironmentConfiguration { MaxDatabases = 5, MapSize = 1024 * 1024 * 100 });
        return this;
    }

    /// <summary>
    /// Builds a configured queue instance.
    /// </summary>
    /// <returns>A fully configured Queue instance.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown if required configuration elements are missing.
    /// </exception>
    /// <remarks>
    /// This method creates and configures all the components needed for a functioning queue:
    /// - Message store
    /// - Network sender and receiver
    /// - Protocols
    /// - Serializer
    /// 
    /// The queue is created but not started. Call Start() on the returned queue
    /// to begin message processing.
    /// </remarks>
    public Queue BuildQueue()
    {
        if (_store == null)
            throw new ArgumentNullException(nameof(_store), "Storage has not been configured. Are you missing a call to StoreMessagesWith?");

        if (_endpoint == null)
            throw new ArgumentNullException(nameof(_endpoint), "Endpoint has not been configured. Are you missing a call to ReceiveMessageAt?");

        if (_logger == null)
            throw new ArgumentNullException(nameof(_logger), "Logger has not been configured. Are you missing a call to LogWith or WithDefaults?");

        if (_sendingSecurity == null || _receivingSecurity == null)
            throw new ArgumentNullException(nameof(_sendingSecurity), "Security has not been configured. Are you missing a call to SecureTransportWith or WithDefaults?");

        var serializer = new MessageSerializer();
        var store = _store();
        _sendingProtocol ??= new SendingProtocol(store, _sendingSecurity, serializer, _logger);
        _receivingProtocol ??= new ReceivingProtocol(store, _receivingSecurity, serializer, new Uri($"lq.tcp://{_endpoint}"), _logger);

        var receiver = new Receiver(_endpoint, _receivingProtocol, _logger);
        var sender = new Sender(_sendingProtocol, _logger, _timeoutBatchAfter);
        var queue = new Queue(receiver, sender, store, _logger);
        return queue;
    }

    /// <summary>
    /// Builds, creates, and starts a queue with the specified name.
    /// </summary>
    /// <param name="queueName">The name of the queue to create.</param>
    /// <returns>A fully configured and running Queue instance.</returns>
    /// <remarks>
    /// This is a convenience method that:
    /// 1. Builds the queue
    /// 2. Creates a queue with the specified name
    /// 3. Starts the queue's processing operations
    /// 
    /// The queue is ready to send and receive messages immediately after this call.
    /// </remarks>
    public Queue BuildAndStart(string queueName)
    {
        var queue = BuildQueue();
        queue.CreateQueue(queueName);
        queue.Start();
        return queue;
    }
}