using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1;

public class ReceivingProtocol(IMessageStore store, IStreamSecurity security, IMessageSerializer serializer, Uri receivingUri, ILogger logger)
    : ProtocolBase(logger), IReceivingProtocol
{
    public async Task<IList<Message>> ReceiveMessagesAsync(Stream stream, CancellationToken cancellationToken)
    {
        using var doneCancellation = new CancellationTokenSource();
        var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, cancellationToken);
        try
        {
            return await ReceiveMessagesAsyncImpl(stream, linkedCancel.Token).ConfigureAwait(false);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw new ProtocolViolationException("Unable to receive messages, malformed message received");
        }
        catch (OutOfMemoryException)
        {
            throw new ProtocolViolationException("Unable to receive messages, insufficient data available");
        }
        finally
        {
            await doneCancellation.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task<IList<Message>> ReceiveMessagesAsyncImpl(Stream stream, CancellationToken cancellationToken)
    {
        var pipe = new Pipe();
        stream = await security.Apply(receivingUri, stream).ConfigureAwait(false);
        var receivingTask = ReceiveIntoBuffer(pipe.Writer, stream, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            return [];

        // Read the message length (4 bytes, little-endian) using BCL primitives
        var lengthResult = await pipe.Reader.ReadAtLeastAsync(sizeof(int), cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32LittleEndian(lengthResult.Buffer.FirstSpan);
        pipe.Reader.AdvanceTo(lengthResult.Buffer.GetPosition(sizeof(int)));

        Logger.ReceiverReceivedLength(length);
        if (length <= 0 || cancellationToken.IsCancellationRequested)
            return [];
        var result = await pipe.Reader.ReadAtLeastAsync(length, cancellationToken).ConfigureAwait(false);
        if (result.Buffer.Length != length)
            throw new ProtocolViolationException($"Protocol violation: received length of {length} bytes, but {result.Buffer.Length} bytes were available");
        if (cancellationToken.IsCancellationRequested)
            return [];

        // Zero-copy storage path: parse wire format and store directly
        // For LMDB stores, this avoids deserialize-then-reserialize
        // For other stores, the interface provides a default implementation
        var bufferArray = result.Buffer.IsSingleSegment
            ? result.Buffer.First.ToArray()
            : result.Buffer.ToArray();

        // Allocate buffer for raw message info (reuse pool for large batches)
        var rawInfoBuffer = ArrayPool<RawMessageInfo>.Shared.Rent(256);
        IList<Message> messages;
        try
        {
            int messageCount;
            try
            {
                messageCount = WireFormatSplitter.SplitBatch(bufferArray, 0, bufferArray.Length, rawInfoBuffer);
            }
            catch (Exception ex)
            {
                Logger.ProtocolReadMessagesError(ex);
                throw new ProtocolViolationException("Failed to parse wire format: " + ex.Message);
            }

            if (messageCount == 0)
                throw new ProtocolViolationException("No messages received");

            try
            {
                store.StoreRawIncoming(rawInfoBuffer, messageCount, serializer);
            }
            catch (QueueDoesNotExistException)
            {
                await SendQueueNotFound(stream, cancellationToken);
                throw;
            }
            catch (Exception)
            {
                await SendProcessingError(stream, cancellationToken);
                throw;
            }

            // Deserialize for the return value (needed for downstream channel)
            try
            {
                messages = serializer.ReadMessages(result.Buffer);
            }
            catch (EndOfStreamException)
            {
                Logger.ProtocolReadError();
                throw new ProtocolViolationException("Failed to read messages, possible disconnected client or malformed request");
            }
        }
        finally
        {
            ArrayPool<RawMessageInfo>.Shared.Return(rawInfoBuffer);
        }

        if (!cancellationToken.IsCancellationRequested)
            await SendReceived(stream, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            return [];
        var acknowledgeTask = ReadAcknowledged(pipe, cancellationToken);
        await Task.WhenAny(acknowledgeTask.AsTask(), receivingTask.AsTask()).ConfigureAwait(false);
        return messages;
    }

    private static async ValueTask SendReceived(Stream stream, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Constants.ReceivedMemory, cancellationToken)
            .ConfigureAwait(false);
    }
    
    private async ValueTask SendQueueNotFound(Stream stream, CancellationToken cancellationToken)
    {
        try
        {
            await stream.WriteAsync(Constants.QueueDoesNotExistMemory, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.ProtocolQueueNotFoundError(ex);
        }
    }

    private async ValueTask SendProcessingError(Stream stream, CancellationToken cancellationToken)
    {
        try
        {
            await stream.WriteAsync(Constants.ProcessingFailureMemory, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.ProtocolProcessingError(ex);
        }
    }

    private static async ValueTask ReadAcknowledged(Pipe pipe, CancellationToken cancellationToken)
    {
        var ackLength = Constants.AcknowledgedBuffer.Length;
        var result = await pipe.Reader.ReadAtLeastAsync(ackLength, cancellationToken).ConfigureAwait(false);
        var sequence = result.Buffer;
        if (!SequenceEqual(ref sequence, Constants.AcknowledgedBuffer))
        {
            throw new ProtocolViolationException("Didn't receive expected acknowledgement");
        }
    }
}