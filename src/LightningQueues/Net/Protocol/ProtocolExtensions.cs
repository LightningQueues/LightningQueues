using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net.Protocol;

internal static class ProtocolExtensions
{
    internal static async ValueTask ReceiveIntoBuffer(this PipeWriter writer, Stream stream, ILogger logger,
        CancellationToken cancellationToken)
    {
        const int minimumBufferSize = 512;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken);
                if (bytesRead == 0)
                {
                    break;
                }

                writer.Advance(bytesRead);

                var result = await writer.FlushAsync(cancellationToken);

                if (result.IsCompleted)
                {
                    break;
                }
            }

            await writer.CompleteAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading from stream");
            if (!cancellationToken.IsCancellationRequested)
                throw;
        }
    }

    internal static bool SequenceEqual(this ref ReadOnlySequence<byte> sequence, byte[] target)
    {
        var targetSpan = target.AsSpan();
        Span<byte> sequenceSpan = stackalloc byte[targetSpan.Length];
        sequence.CopyTo(sequenceSpan);
        return targetSpan.SequenceEqual(sequenceSpan);
    }
}