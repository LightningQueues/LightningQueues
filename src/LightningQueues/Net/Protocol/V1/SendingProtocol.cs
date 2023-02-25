using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Serialization;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1;

public class SendingProtocol : ISendingProtocol
{
    private readonly IMessageStore _store;
    private readonly ILogger _logger;

    public SendingProtocol(IMessageStore store, ILogger logger)
    {
        _store = store;
        _logger = logger;
    }

    public IObservable<OutgoingMessage> Send(OutgoingMessageBatch batch)
    {
        return from outgoing in Observable.Return(batch)
            from stream in outgoing.Stream
            let messageBytes = outgoing.Messages.Serialize()
            from l in WriteLength(stream, messageBytes.Length).Do(_ => _logger.DebugFormat("Writing {0} message length to {1}", messageBytes.Length, outgoing.Destination))
            from m in WriteMessages(stream, messageBytes).Do(_ => _logger.DebugFormat("Wrote messages to destination {0}", outgoing.Destination))
            from r in ReadReceived(stream).Do(_ => _logger.DebugFormat("Read received bytes from {0}", outgoing.Destination))
            from a in WriteAcknowledgement(stream).Do(_ => _store.SuccessfullySent(outgoing.Messages.ToArray())).Do(_ => _logger.DebugFormat("Wrote acknowledgement to {0}", outgoing.Destination))
            from message in outgoing.Messages
            select message;
    }

    public async ValueTask<IEnumerable<OutgoingMessage>> Send(Uri destination, IEnumerable<OutgoingMessage> batch)
    {
        await Task.Delay(10);
        return batch;
    }

    public IObservable<Unit> WriteLength(Stream stream, int length)
    {
        var lengthBytes = BitConverter.GetBytes(length);
        return Observable.FromAsync(async () => await stream.WriteAsync(lengthBytes, 0, lengthBytes.Length).ConfigureAwait(false));
    }

    public IObservable<Unit> WriteMessages(Stream stream, byte[] messageBytes)
    {
        return Observable.FromAsync(async () => await stream.WriteAsync(messageBytes, 0, messageBytes.Length).ConfigureAwait(false));
    }

    public IObservable<Unit> ReadReceived(Stream stream)
    {
        return Observable.FromAsync(async () =>
        {
            try
            {
                var bytes = await stream.ReadBytesAsync(Constants.ReceivedBuffer.Length).ConfigureAwait(false);
                if (bytes.SequenceEqual(Constants.ReceivedBuffer))
                {
                    return true;
                }
                if (bytes.SequenceEqual(Constants.SerializationFailureBuffer))
                {
                    _logger.Error("Serialization is confused and did not return received buffer", new IOException("Failed to send messages, received serialization failed message."));
                }
                if (bytes.SequenceEqual(Constants.QueueDoesNotExistBuffer))
                {
                    _logger.Error("Destination queue does not exist", new QueueDoesNotExistException());
                }
                return false;
            }
            catch (Exception ex)
            {
                _logger.Info("Error while trying to read received buffer " + ex);
                return false;
            }
                
        }).Where(x => x).Select(_ => Unit.Default);
    }

    public IObservable<Unit> WriteAcknowledgement(Stream stream)
    {
        return Observable.FromAsync(async () => await stream.WriteAsync(Constants.AcknowledgedBuffer, 0, Constants.AcknowledgedBuffer.Length).ConfigureAwait(false));
    }
}