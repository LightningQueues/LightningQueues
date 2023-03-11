using System;
using System.Collections.Generic;
using System.IO;
using DotNext.IO.Pipelines;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DotNext.IO;
using Microsoft.Extensions.Logging;
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

    public async IAsyncEnumerable<Message> ReceiveMessagesAsync(Stream stream, 
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var pipe = new Pipe();
        stream = await _security.Apply(_receivingUri, stream);
        pipe.Writer.ReceiveIntoBuffer(stream, false, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            yield break;
        var length = await pipe.Reader.ReadInt32Async(true, cancellationToken);
        if (length <= 0 || cancellationToken.IsCancellationRequested)
            yield break;
        _logger.ReceiverReceivedLength(length);
        var result = await pipe.Reader.ReadAtLeastAsync(length, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            yield break;
        var reader = new SequenceReader(result.Buffer);
        var messages = ReadMessages(reader);

        using var enumerator = messages.GetEnumerator();
        var hasResult = true;
        while (hasResult)
        {
            hasResult = enumerator.MoveNext();
            var msg = hasResult ? enumerator.Current : null;

            if (msg == null)
                continue;
            try
            {
                _store.StoreIncomingMessages(msg);
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

            yield return msg;
        }

        if (cancellationToken.IsCancellationRequested)
            yield break;
        await SendReceived(stream, cancellationToken);
        if (cancellationToken.IsCancellationRequested)
            yield break;
        await ReadAcknowledged(pipe, cancellationToken);
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
        var ackLength = Constants.AcknowledgedBuffer.Length;
        var result = await pipe.Reader.ReadAtLeastAsync(ackLength, cancellationToken);
        var sequence = result.Buffer;
        if (!sequence.SequenceEqual(Constants.AcknowledgedBuffer))
        {
            throw new ProtocolViolationException("Didn't receive expected acknowledgement");
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
}