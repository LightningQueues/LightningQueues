using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using DotNext.Buffers;
using LightningQueues.Logging;
using LightningQueues.Net.Security;
using LightningQueues.Serialization;
using LightningQueues.Storage;

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

    public async ValueTask SendAsync(Uri destination,Stream stream, 
        IEnumerable<OutgoingMessage> batch, CancellationToken token)
    {
        stream = await _security.Apply(destination, stream);
        using var writer = new PooledBufferWriter<byte>();
        var messages = batch.ToList();
        writer.WriteMessages(messages);
        await stream.WriteAsync(BitConverter.GetBytes(writer.WrittenMemory.Length), token);
        _logger.Debug("Writing message batch to destination");
        await stream.WriteAsync(writer.WrittenMemory, token);
        _logger.Debug("Successfully wrote message batch to destination");
        await ReadReceived(stream, token);
        _logger.Debug("Successfully read received message");
        await WriteAcknowledgement(stream, token);
        _logger.Debug("Successfully wrote acknowledgement");
        _store.SuccessfullySent(messages);
        _logger.Debug("Stored that messages were successful");
    }

    private static async ValueTask ReadReceived(Stream stream, CancellationToken token)
    {
        var bytes = await stream.ReadBytesAsync(Constants.ReceivedBuffer.Length).ConfigureAwait(false);
        if (bytes.SequenceEqual(Constants.ReceivedBuffer))
        {
            return;
        }
        if (bytes.SequenceEqual(Constants.SerializationFailureBuffer))
        {
            throw new SerializationException("The destination returned serialization error");
        }
        if (bytes.SequenceEqual(Constants.QueueDoesNotExistBuffer))
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