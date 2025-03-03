using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net.Protocol;

public abstract class ProtocolBase
{
    protected readonly ILogger Logger;

    protected ProtocolBase(ILogger logger)
    {
        Logger = logger;
    }
    protected async ValueTask ReceiveIntoBuffer(PipeWriter writer, Stream stream, CancellationToken cancellationToken)
    {
        const int minimumBufferSize = 512;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = writer.GetMemory(minimumBufferSize);
                var bytesRead = await stream.ReadAsync(memory, cancellationToken).ConfigureAwait(false);
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

            await writer.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.ProtocolStreamError(ex);
            if (!cancellationToken.IsCancellationRequested)
                throw;
        }
    }

    protected static bool SequenceEqual(ref ReadOnlySequence<byte> sequence, byte[] target)
    {
        var targetSpan = target.AsSpan();
        Span<byte> sequenceSpan = stackalloc byte[targetSpan.Length];
        sequence.CopyTo(sequenceSpan);
        return targetSpan.SequenceEqual(sequenceSpan);
    }
}