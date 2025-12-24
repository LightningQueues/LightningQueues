using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Tcp;

public class Sender : IDisposable
{
    private readonly ISendingProtocol _protocol;
    private readonly IMessageSerializer _serializer;
    private readonly Channel<OutgoingMessageFailure> _failedToSend;
    private readonly ILogger _logger;
    private readonly TimeSpan _sendTimeout;
    
    // Connection pooling for improved performance
    private readonly ConcurrentDictionary<string, ConcurrentQueue<PooledTcpClient>> _connectionPools = new();
    private readonly Timer _poolCleanupTimer;
    private const int MaxConnectionsPerEndpoint = 5;
    private const int ConnectionIdleTimeoutMs = 30000;
    
    // DNS resolution cache for improved performance
    private readonly ConcurrentDictionary<string, CachedDnsEntry> _dnsCache = new();
    private const int DnsCacheTimeoutMs = 300000; // 5 minutes

    private class CachedDnsEntry
    {
        public required IPAddress[] Addresses { get; set; }
        public DateTime CachedAt { get; set; }
        public bool IsExpired => DateTime.UtcNow - CachedAt > TimeSpan.FromMilliseconds(DnsCacheTimeoutMs);
    }

    public Sender(ISendingProtocol protocol, IMessageSerializer serializer, ILogger logger, TimeSpan sendTimeout)
    {
        _protocol = protocol;
        _serializer = serializer;
        _logger = logger;
        _sendTimeout = sendTimeout;
        _failedToSend = Channel.CreateUnbounded<OutgoingMessageFailure>();

        // Start cleanup timer to periodically clean up idle connections
        _poolCleanupTimer = new Timer(CleanupIdleConnections, null,
            TimeSpan.FromMilliseconds(ConnectionIdleTimeoutMs),
            TimeSpan.FromMilliseconds(ConnectionIdleTimeoutMs));
    }

    private class PooledTcpClient : IDisposable
    {
        public Socket Socket { get; }
        public NetworkStream Stream { get; }
        public DateTime LastUsed { get; set; }
        public string Endpoint { get; }

        public PooledTcpClient(Socket socket, string endpoint)
        {
            Socket = socket;
            Stream = new NetworkStream(socket, ownsSocket: false);
            Endpoint = endpoint;
            LastUsed = DateTime.UtcNow;
        }

        public bool IsConnected => Socket.Connected;
        
        public bool IsExpired => DateTime.UtcNow - LastUsed > TimeSpan.FromMilliseconds(ConnectionIdleTimeoutMs);

        public void UpdateLastUsed() => LastUsed = DateTime.UtcNow;

        public void Dispose()
        {
            Stream?.Dispose();
            Socket?.Dispose();
        }
    }

    private void CleanupIdleConnections(object? state)
    {
        // Clean up connection pools - avoid List allocation by processing immediately
        foreach (var kvp in _connectionPools)
        {
            var pool = kvp.Value;
            
            // Process connections one by one without collecting them
            var processedCount = 0;
            var maxToProcess = 10; // Limit processing to prevent long blocking
            
            while (processedCount < maxToProcess && pool.TryDequeue(out var pooledClient))
            {
                if (pooledClient.IsExpired || !pooledClient.IsConnected)
                {
                    pooledClient.Dispose();
                }
                else
                {
                    // Put back non-expired connection and stop processing this pool
                    pool.Enqueue(pooledClient);
                    break;
                }
                processedCount++;
            }
        }
        
        // Clean up DNS cache - avoid LINQ allocation by processing directly
        foreach (var kvp in _dnsCache)
        {
            if (kvp.Value.IsExpired)
            {
                _dnsCache.TryRemove(kvp.Key, out _);
            }
        }
    }

    private async Task<IPAddress?> ResolveDnsAsync(string hostName, CancellationToken cancellationToken)
    {
        // Check cache first
        if (_dnsCache.TryGetValue(hostName, out var cachedEntry) && !cachedEntry.IsExpired)
        {
            return cachedEntry.Addresses[0]; // Return first address
        }

        // Resolve DNS and cache result
        try
        {
            var addresses = await Dns.GetHostAddressesAsync(hostName, cancellationToken).ConfigureAwait(false);
            if (addresses.Length > 0)
            {
                _dnsCache[hostName] = new CachedDnsEntry
                {
                    Addresses = addresses,
                    CachedAt = DateTime.UtcNow
                };
                return addresses[0];
            }
        }
        catch
        {
            // Fall back to default resolution if caching fails
        }

        // Fallback to synchronous resolution
        var fallbackAddresses = Dns.GetHostAddresses(hostName);
        return fallbackAddresses.Length > 0 ? fallbackAddresses[0] : null;
    }

    private async Task<PooledTcpClient> GetConnectionAsync(Uri uri, CancellationToken cancellationToken)
    {
        var endpoint = $"{uri.Host}:{uri.Port}";
        var pool = _connectionPools.GetOrAdd(endpoint, _ => new ConcurrentQueue<PooledTcpClient>());

        // Try to get an existing connection
        while (pool.TryDequeue(out var pooledClient))
        {
            if (pooledClient.IsConnected && !pooledClient.IsExpired)
            {
                pooledClient.UpdateLastUsed();
                return pooledClient;
            }
            pooledClient.Dispose(); // Clean up invalid connection
        }

        // Create new connection using Socket for better performance
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            if (uri.IsLoopback || Dns.GetHostName() == uri.Host)
            {
                await socket.ConnectAsync(IPAddress.Loopback, uri.Port, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                // Use cached DNS resolution for better performance
                var resolvedAddress = await ResolveDnsAsync(uri.Host, cancellationToken).ConfigureAwait(false);
                if (resolvedAddress != null)
                {
                    await socket.ConnectAsync(resolvedAddress, uri.Port, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    // Fallback to hostname if DNS resolution fails
                    await socket.ConnectAsync(uri.Host, uri.Port, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            
            // Configure socket for optimal performance
            socket.NoDelay = true; // Disable Nagle's algorithm for low latency
            socket.ReceiveBufferSize = 8192;
            socket.SendBufferSize = 8192;
            
            return new PooledTcpClient(socket, endpoint);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private void ReturnConnection(PooledTcpClient pooledClient)
    {
        if (pooledClient?.IsConnected == true && !pooledClient.IsExpired)
        {
            var pool = _connectionPools.GetOrAdd(pooledClient.Endpoint, _ => new ConcurrentQueue<PooledTcpClient>());
            
            // Limit pool size to prevent memory leaks
            var currentSize = 0;
            var tempList = new List<PooledTcpClient>();
            
            while (pool.TryDequeue(out var existing) && currentSize < MaxConnectionsPerEndpoint - 1)
            {
                if (existing.IsConnected && !existing.IsExpired)
                {
                    tempList.Add(existing);
                    currentSize++;
                }
                else
                {
                    existing.Dispose();
                }
            }
            
            // Return valid connections to pool
            foreach (var connection in tempList)
            {
                pool.Enqueue(connection);
            }
            
            // Add the current connection if there's space
            if (currentSize < MaxConnectionsPerEndpoint)
            {
                pool.Enqueue(pooledClient);
            }
            else
            {
                pooledClient.Dispose();
            }
        }
        else
        {
            pooledClient?.Dispose();
        }
    }

    public Channel<OutgoingMessageFailure> FailedToSend() => _failedToSend;

    public async ValueTask StartSendingAsync(IMessageStore messageStore, int batchSize = 50, TimeSpan pollInterval = default, CancellationToken cancellationToken = default)
    {
        pollInterval = pollInterval == default ? TimeSpan.FromMilliseconds(200) : pollInterval;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var batch = new List<RawOutgoingMessage>(batchSize);
                var batchCount = 0;

                // Read raw messages from storage (zero-copy path - no deserialization)
                foreach (var rawMessage in messageStore.PersistedOutgoingRaw())
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    batch.Add(rawMessage);
                    batchCount++;

                    if (batchCount >= batchSize)
                        break;
                }

                // If no messages, wait before polling again
                if (batchCount == 0)
                {
                    await Task.Delay(pollInterval, cancellationToken).ConfigureAwait(false);
                    continue;
                }
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                linked.CancelAfter(_sendTimeout);

                // Group messages by destination AND queue to prevent batching messages to different queues together
                // This ensures that a QueueDoesNotExistException for one queue doesn't affect messages to other queues
                var destinationQueueGroups = batch
                    .GroupBy(m => new
                    {
                        Destination = WireFormatReader.GetDestinationUri(in m),
                        Queue = WireFormatReader.GetQueueName(in m)
                    })
                    .ToList();

                // Process each destination+queue group separately
                foreach (var group in destinationQueueGroups)
                {
                    var uriString = group.Key.Destination;
                    var uri = new Uri(uriString);
                    var rawMessagesForDestination = group.ToList();
                    PooledTcpClient? pooledClient = null;

                    try
                    {
                        pooledClient = await GetConnectionAsync(uri, linked.Token).ConfigureAwait(false);

                        // Send using zero-copy path (SendRawAsync handles deletion on success)
                        await _protocol.SendRawAsync(uri, pooledClient.Stream, rawMessagesForDestination, linked.Token)
                            .ConfigureAwait(false);

                        // Return connection to pool on success
                        ReturnConnection(pooledClient);
                        pooledClient = null; // Prevent disposal in finally
                    }
                    catch (SocketException ex) when (ex.SocketErrorCode == SocketError.HostNotFound)
                    {
                        _logger.SenderSendingError(uri, ex);
                        // Deserialize for failure handling (rare path, performance not critical)
                        var messages = DeserializeRawMessages(rawMessagesForDestination);
                        var failed = new OutgoingMessageFailure
                        {
                            Messages = messages,
                            ShouldRetry = false
                        };
                        await _failedToSend.Writer.WriteAsync(failed, cancellationToken).ConfigureAwait(false);
                    }
                    catch (QueueDoesNotExistException ex)
                    {
                        _logger.SenderQueueDoesNotExistError(uri, ex);
                        // Deserialize for failure handling (rare path, performance not critical)
                        var messages = DeserializeRawMessages(rawMessagesForDestination);
                        // When queue doesn't exist, separate messages by queue and handle each queue's retry logic independently
                        var messagesByQueue = messages.GroupBy(m => m.QueueString);
                        foreach (var queueGroup in messagesByQueue)
                        {
                            var failed = new OutgoingMessageFailure
                            {
                                Messages = queueGroup.ToList(),
                                ShouldRetry = true // Let the retry policy handle max attempts per queue
                            };
                            await _failedToSend.Writer.WriteAsync(failed, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.SenderSendingError(uri, ex);
                        // Deserialize for failure handling (rare path, performance not critical)
                        var messages = DeserializeRawMessages(rawMessagesForDestination);
                        var failed = new OutgoingMessageFailure
                        {
                            Messages = messages,
                            ShouldRetry = true
                        };
                        await _failedToSend.Writer.WriteAsync(failed, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        // Dispose connection on error (if not returned to pool)
                        pooledClient?.Dispose();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected during shutdown - don't log
            }
            catch (Exception ex)
            {
                _logger.SenderSendingLoopError(ex);
            }
        }
    }

    /// <summary>
    /// Deserializes raw messages back to Message objects for failure handling.
    /// This is only called in error paths, so performance is not critical.
    /// </summary>
    private List<Message> DeserializeRawMessages(List<RawOutgoingMessage> rawMessages)
    {
        var messages = new List<Message>(rawMessages.Count);
        foreach (var raw in rawMessages)
        {
            var message = _serializer.ToMessage(raw.FullMessage.Span);
            messages.Add(message);
        }
        return messages;
    }

    public void Dispose()
    {
        _logger.SenderDisposing();
        
        try
        {
            // Complete the channel to prevent further sends
            _failedToSend.Writer.TryComplete();
            
            // Dispose the cleanup timer
            _poolCleanupTimer?.Dispose();
            
            // Dispose all pooled connections
            foreach (var kvp in _connectionPools)
            {
                while (kvp.Value.TryDequeue(out var pooledClient))
                {
                    pooledClient.Dispose();
                }
            }
            _connectionPools.Clear();
            
            // Clear DNS cache
            _dnsCache.Clear();
        }
        catch (Exception ex)
        {
            // Just log and continue with disposal
            _logger.SenderDisposalError(ex);
        }

        GC.SuppressFinalize(this);
    }
}