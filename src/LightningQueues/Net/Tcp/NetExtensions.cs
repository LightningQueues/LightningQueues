using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace LightningQueues.Net.Tcp;

public static class NetExtensions
{
    public static async ValueTask<List<T>> ReadBatchAsync<T>(this ChannelReader<T> channelReader,
        int batchSize, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channelReader);
        if (batchSize < 1) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);
        List<T> buffer = new();
        while (true)
        {
            var token = buffer.Count == 0 ? cancellationToken : linked.Token;
            T item;
            try
            {
                item = await channelReader.ReadAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break; // The cancellation was induced by timeout (ignore it)
            }
            catch (ChannelClosedException)
            {
                break; // We are already persistent, will recover on the next start
            }
            buffer.Add(item);
            if (buffer.Count >= batchSize) break;
        }
        return buffer;
    }
}