using System;
using System.Collections.Generic;
using System.IO;
using DotNext.IO.Pipelines;
using System.IO.Pipelines;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1;

public class ReceivingProtocol : ProtocolBase, IReceivingProtocol
{
    private readonly IMessageStore _store;
    private readonly IStreamSecurity _security;
    private readonly IMessageSerializer _serializer;
    private readonly Uri _receivingUri;

    public ReceivingProtocol(IMessageStore store, IStreamSecurity security, IMessageSerializer serializer, Uri receivingUri, ILogger logger) 
        : base(logger)
    {
        _store = store;
        _security = security;
        _serializer = serializer;
        _receivingUri = receivingUri;
    }

    public async Task<IList<Message>> ReceiveMessagesAsync(Stream stream, CancellationToken cancellationToken)
    {
        using var doneCancellation = new CancellationTokenSource();
        var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, cancellationToken);
        try
        {
            return await ReceiveMessagesAsyncImpl(stream, linkedCancel.Token).ConfigureAwait(false);
        }
        finally
        {
            await doneCancellation.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task<IList<Message>> ReceiveMessagesAsyncImpl(Stream stream, CancellationToken cancellationToken)
    {
        var pipe = new Pipe();
        stream = await _security.Apply(_receivingUri, stream).ConfigureAwait(false);
        var receivingTask = ReceiveIntoBuffer(pipe.Writer, stream, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            return null;
        var length = await pipe.Reader.ReadLittleEndianAsync<int>(cancellationToken).ConfigureAwait(false);
        Logger.ReceiverReceivedLength(length);
        if (length <= 0 || cancellationToken.IsCancellationRequested)
            return null;
        var result = await pipe.Reader.ReadAtLeastAsync(length, cancellationToken).ConfigureAwait(false);
        if (result.Buffer.Length != length)
            throw new ProtocolViolationException($"Protocol violation: received length of {length} bytes, but {result.Buffer.Length} bytes were available");
        if (cancellationToken.IsCancellationRequested)
            return null;
        IList<Message> messages;
        try
        {
            messages = _serializer.ReadMessages(result.Buffer);
        }
        catch (EndOfStreamException)
        {
            Logger.ProtocolReadError();
            throw new ProtocolViolationException("Failed to read messages, possible disconnected client or malformed request");
        }
        catch (Exception ex)
        {
            Logger.ProtocolReadMessagesError(ex);
            throw;
        }
        
        if(messages.Count == 0)
            throw new ProtocolViolationException("No messages received");

        try
        {
            _store.StoreIncoming(messages);
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

        if (!cancellationToken.IsCancellationRequested)
            await SendReceived(stream, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            return null;
        var acknowledgeTask = ReadAcknowledged(pipe, cancellationToken);
        await Task.WhenAny(acknowledgeTask.AsTask(), receivingTask.AsTask()).ConfigureAwait(false);
        return messages;
    }

    private static async ValueTask SendReceived(Stream stream, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Constants.ReceivedBuffer.AsMemory(), cancellationToken)
            .ConfigureAwait(false);
    }
    
    private async ValueTask SendQueueNotFound(Stream stream, CancellationToken cancellationToken)
    {
        try
        {
            await stream.WriteAsync(Constants.QueueDoesNotExistBuffer.AsMemory(), cancellationToken)
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
            await stream.WriteAsync(Constants.ProcessingFailureBuffer.AsMemory(), cancellationToken)
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