using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using DotNext.IO.Pipelines;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DotNext.IO;
using LightningQueues.Logging;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1;

public class ReceivingProtocol : IReceivingProtocol
{
    private readonly IMessageStore _store;
    private readonly IStreamSecurity _security;
    private readonly Uri _receivingUri;
    private readonly ILogger _logger;

    public ReceivingProtocol(IMessageStore store, IStreamSecurity security, Uri receivingUri, ILogger logger)
    {
        _store = store;
        _security = security;
        _receivingUri = receivingUri;
        _logger = logger;
    }
    
    public async IAsyncEnumerable<Message> ReceiveMessagesAsync(EndPoint endpoint, Stream stream, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var cancelReading = new CancellationTokenSource();
        var doneCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cancelReading.Token);
        ValueTask receivingTask;
        var pipe = new Pipe();
        IEnumerable<Message> messages;
        try
        {
            stream = await _security.Apply(_receivingUri, stream);
            receivingTask = ReceiveIntoBuffer(pipe.Writer, stream, false, doneCancellation.Token);
            if(cancellationToken.IsCancellationRequested)
                yield break;
            var length = await pipe.Reader.ReadInt32Async(true, cancellationToken);
            if(length <= 0 || cancellationToken.IsCancellationRequested)
                yield break;
            _logger.Debug($"Received length value of {length}");
            var result = await pipe.Reader.ReadAtLeastAsync(length, cancellationToken);
            if(cancellationToken.IsCancellationRequested)
                yield break;
            var reader = new SequenceReader(result.Buffer);
            messages = ReadMessages(reader);
        }
        catch (Exception ex)
        {
            _logger.Error($"Error reading messages from {endpoint}", ex);
            yield break;
        }

        using var enumerator = messages.GetEnumerator();
        var hasResult = true;
        while (hasResult)
        {
            Message msg;
            try
            {
                hasResult = enumerator.MoveNext();
                msg = hasResult ? enumerator.Current : null;
            }
            catch (Exception ex)
            {
                _logger.Error("Error reading messages", ex);
                yield break;
            }

            if (msg == null)
                continue;
            try
            {
                _store.StoreIncomingMessages(msg);
            }
            catch (QueueDoesNotExistException)
            {
                _logger.Info($"Queue {msg.Queue} not found for {msg.Id}");
                await SendQueueNotFound(stream, cancellationToken);
                continue;
            }
            catch (Exception ex)
            {
                _logger.Error($"Error persisting {msg.Id}", ex);
                await SendProcessingError(stream, cancellationToken);
                continue;
            }

            yield return msg;
        }

        try
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;
            await SendReceived(stream, cancellationToken);
            if (cancellationToken.IsCancellationRequested)
                yield break;
            await ReadAcknowledged(pipe, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.Error("Error finishing protocol acknowledgement", ex);
        }
        cancelReading.Cancel();
        await receivingTask;
    }
    
    private static async ValueTask SendReceived(Stream stream, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Constants.ReceivedBuffer.AsMemory(), cancellationToken);
    }
    
    private static async ValueTask SendQueueNotFound(Stream stream, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Constants.QueueDoesNotExistBuffer.AsMemory(), cancellationToken);
    }
    
    private static async ValueTask SendProcessingError(Stream stream, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Constants.ProcessingFailureBuffer.AsMemory(), cancellationToken);
    }
    
    private static async ValueTask ReadAcknowledged(Pipe pipe, CancellationToken cancellationToken)
    {
        bool ValidateAck(ReadOnlySequence<byte> sequence)
        {
            var ack = Constants.AcknowledgedBuffer.AsSpan();
            Span<byte> sequenceSpan = stackalloc byte[Constants.AcknowledgedBuffer.Length];
            sequence.CopyTo(sequenceSpan);
            return ack.SequenceEqual(sequenceSpan);
        }
        var ackLength = Constants.AcknowledgedBuffer.Length;
        var result = await pipe.Reader.ReadAtLeastAsync(ackLength, cancellationToken);
        var sequence = result.Buffer;
        if (!ValidateAck(sequence))
        {
            throw new Exception("Todo better error");
        }
    }

    private static IEnumerable<Message> ReadMessages(SequenceReader reader)
    {
        var numberOfMessages = reader.ReadInt32(true);
        for (var i = 0; i < numberOfMessages; ++i)
        {
            var msg = reader.ReadMessage<Message>();
            yield return msg;
        }
    }

    private static async ValueTask ReceiveIntoBuffer(PipeWriter writer, Stream stream, bool shouldComplete,
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
}