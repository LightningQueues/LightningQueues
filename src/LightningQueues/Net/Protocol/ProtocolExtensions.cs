using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net.Protocol;

internal static class ProtocolExtensions
{
    internal static async ValueTask ReceiveIntoBuffer(this PipeWriter writer, Stream stream, bool shouldComplete,
        CancellationToken cancellationToken)
    {
        const int minimumBufferSize = 512;

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

        if (shouldComplete)
        {
            await writer.CompleteAsync();
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