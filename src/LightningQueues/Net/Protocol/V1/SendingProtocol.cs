using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using DotNext.Buffers;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net.Protocol.V1;

public class SendingProtocol : ISendingProtocol
{
    private readonly IMessageStore _store;
    private readonly IStreamSecurity _security;
    private readonly ILogger _logger;

    public SendingProtocol(IMessageStore store, IStreamSecurity security, ILogger logger)
    {
        _store = store;
        _security = security;
        _logger = logger;
    }

    public async ValueTask SendAsync(Uri destination, Stream stream,
        IEnumerable<OutgoingMessage> batch, CancellationToken token)
    {
        stream = await _security.Apply(destination, stream);
        using var writer = new PooledBufferWriter<byte>();
        var messages = batch.ToList();
        writer.WriteOutgoingMessages(messages);
        await stream.WriteAsync(BitConverter.GetBytes(writer.WrittenMemory.Length), token);
        _logger.SenderWritingMessageBatch();
        await stream.WriteAsync(writer.WrittenMemory, token);
        _logger.SenderSuccessfullyWroteMessageBatch();
        var pipe = new Pipe();
        pipe.Writer.ReceiveIntoBuffer(stream, true, token);
        await ReadReceived(pipe.Reader, token);
        _logger.SenderSuccessfullyReadReceived();
        await WriteAcknowledgement(stream, token);
        _logger.SenderSuccessfullyWroteAcknowledgement();
        _store.SuccessfullySent(messages);
        _logger.SenderStorageSuccessfullySent();
    }

    private static async ValueTask ReadReceived(PipeReader reader, CancellationToken token)
    {
        var result = await reader.ReadAtLeastAsync(Constants.ReceivedBuffer.Length, token).ConfigureAwait(false);
        var buffer = result.Buffer;
        if (buffer.SequenceEqual(Constants.ReceivedBuffer))
        {
            return;
        }
        if (buffer.SequenceEqual(Constants.SerializationFailureBuffer))
        {
            throw new SerializationException("The destination returned serialization error");
        }
        if (buffer.SequenceEqual(Constants.QueueDoesNotExistBuffer))
        {
            throw new QueueDoesNotExistException("Destination queue does not exist.");
        }

        throw new ProtocolViolationException("Unexpected outcome from send operation");
    }

    private static async ValueTask WriteAcknowledgement(Stream stream, CancellationToken token)
    {
        await stream.WriteAsync(Constants.AcknowledgedBuffer.AsMemory(), token).ConfigureAwait(false);
    }
}