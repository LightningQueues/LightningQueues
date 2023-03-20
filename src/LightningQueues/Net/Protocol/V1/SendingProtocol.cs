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

public class SendingProtocol : ProtocolBase, ISendingProtocol
{
    private readonly IMessageStore _store;
    private readonly IStreamSecurity _security;

    public SendingProtocol(IMessageStore store, IStreamSecurity security, ILogger logger) : base(logger)
    {
        _store = store;
        _security = security;
    }

    public async ValueTask SendAsync(Uri destination, Stream stream,
        IEnumerable<OutgoingMessage> batch, CancellationToken token)
    {
        using var doneCancellation = new CancellationTokenSource();
        using var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, token);
        try
        {
            await SendAsyncImpl(destination, stream, batch, linkedCancel.Token).ConfigureAwait(false);
        }
        finally
        {
            doneCancellation.Cancel();
        }
    }

    private async ValueTask SendAsyncImpl(Uri destination, Stream stream,
        IEnumerable<OutgoingMessage> batch, CancellationToken token)
    {
        stream = await _security.Apply(destination, stream).ConfigureAwait(false);
        using var writer = new PooledBufferWriter<byte>();
        var messages = batch.ToList();
        writer.WriteOutgoingMessages(messages);
        await stream.WriteAsync(BitConverter.GetBytes(writer.WrittenMemory.Length), token).ConfigureAwait(false);
        Logger.SenderWritingMessageBatch();
        await stream.WriteAsync(writer.WrittenMemory, token).ConfigureAwait(false);
        Logger.SenderSuccessfullyWroteMessageBatch();
        var pipe = new Pipe();
        var receiveTask = ReceiveIntoBuffer(pipe.Writer, stream, token);
        await ReadReceived(pipe.Reader, token).ConfigureAwait(false);
        Logger.SenderSuccessfullyReadReceived();
        var acknowledgeTask = WriteAcknowledgement(stream, token);
        await Task.WhenAny(acknowledgeTask.AsTask(), receiveTask.AsTask()).ConfigureAwait(false);
        Logger.SenderSuccessfullyWroteAcknowledgement();
        _store.SuccessfullySent(messages);
        Logger.SenderStorageSuccessfullySent();
    }

    private static async ValueTask ReadReceived(PipeReader reader, CancellationToken token)
    {
        var result = await reader.ReadAtLeastAsync(Constants.ReceivedBuffer.Length, token).ConfigureAwait(false);
        var buffer = result.Buffer;
        if (SequenceEqual(ref buffer, Constants.ReceivedBuffer))
        {
            return;
        }
        if (SequenceEqual(ref buffer, Constants.SerializationFailureBuffer))
        {
            throw new SerializationException("The destination returned serialization error");
        }
        if (SequenceEqual(ref buffer, Constants.QueueDoesNotExistBuffer))
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