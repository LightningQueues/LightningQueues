using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net.Protocol.V1;

public class SendingProtocol : ProtocolBase, ISendingProtocol
{
    private readonly IMessageStore _store;
    private readonly IStreamSecurity _security;
    private readonly IMessageSerializer _serializer;

    public SendingProtocol(IMessageStore store, IStreamSecurity security, IMessageSerializer serializer, ILogger logger) : base(logger)
    {
        _store = store;
        _security = security;
        _serializer = serializer;
    }

    public async ValueTask SendAsync(Uri destination, Stream stream, List<Message> batch, CancellationToken token)
    {
        using var doneCancellation = new CancellationTokenSource();
        using var linkedCancel = CancellationTokenSource.CreateLinkedTokenSource(doneCancellation.Token, token);
        try
        {
            await SendAsyncImpl(destination, stream, batch, linkedCancel.Token).ConfigureAwait(false);
        }
        finally
        {
            await doneCancellation.CancelAsync();
        }
    }

    private async ValueTask SendAsyncImpl(Uri destination, Stream stream, List<Message> messages, CancellationToken token)
    {
        stream = await _security.Apply(destination, stream).ConfigureAwait(false);
        
        var memory = _serializer.ToMemory(messages);
        
        await stream.WriteAsync(BitConverter.GetBytes(memory.Length), 0, 4, token).ConfigureAwait(false);
        Logger.SenderWritingMessageBatch();
        await stream.WriteAsync(memory, token).ConfigureAwait(false);
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