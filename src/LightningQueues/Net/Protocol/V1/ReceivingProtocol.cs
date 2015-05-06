using System;
using System.IO;
using System.Reactive.Linq;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net.Protocol.V1
{
    public class ReceivingProtocol : IReceivingProtocol
    {
        readonly IMessageRepository _repository;
        readonly ILogger _logger;
        
        public ReceivingProtocol(IMessageRepository repository, ILogger logger)
        {
            _repository = repository;
            _logger = logger;
        }

        public virtual IObservable<IncomingMessage> ReceiveStream(IObservable<Stream> streams)
        {
            return from stream in streams.Do(x => _logger.Debug("Starting to read stream."))
                   from length in LengthChunk(stream)
                   from messages in MessagesChunk(stream, length).Do(x => StoreMessages(stream, x))
                   from message in messages
                   select message;
        }

        public IObservable<int> LengthChunk(Stream stream)
        {
            return Observable.FromAsync(() => stream.ReadBytesAsync(sizeof(int)))
                .Select(x => BitConverter.ToInt32(x, 0)).Do(x => _logger.Debug($"Read in length value of {x}"))
                .Where(x => x >= 0);
        }

        public IObservable<IncomingMessage[]> MessagesChunk(Stream stream, int length)
        {
            return Observable.FromAsync(() => stream.ReadBytesAsync(length))
                .Select(x => x.ToMessages()).Do(x => _logger.Debug("Successfully read messages"))
                .Catch((Exception ex) =>
                {
                    _logger.Error("Error deserializing messages", ex);
                    return from _ in Observable.FromAsync(() => SendBuffer(stream, Constants.SerializationFailureBuffer))
                           from empty in Observable.Empty<IncomingMessage[]>()
                           select empty;
                });
        }

        private async Task SendBuffer(Stream stream, byte[] buffer)
        {
            await stream.WriteAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
        }

        public async Task StoreMessages(Stream stream, IncomingMessage[] messages)
        {
            var transaction = await BeginTransaction(stream, messages);
            await ReceiveAcknowledgement(stream, transaction);
            await ActualCommit(stream, transaction);
        }

        public async Task<IIncomingTransaction> BeginTransaction(Stream stream, IncomingMessage[] messages)
        {
            try
            {
                var transaction = _repository.StoreMessages(messages);
                return transaction;
            }
            catch(QueueDoesNotExistException)
            {
                _logger.Info("Received a message for a queue that doesn't exist");
                await SendBuffer(stream, Constants.QueueDoesNotExistBuffer);
                throw;
            }
            catch(Exception ex)
            {
                _logger.Error("Error persisting messages in receiver", ex);
                await SendBuffer(stream, Constants.ProcessingFailureBuffer);
                throw;
            }
        }

        public async Task ReceiveAcknowledgement(Stream stream, IIncomingTransaction transaction)
        {
            try
            {
                await SendBuffer(stream, Constants.ReceivedBuffer);

                var ackBuffer = await stream.ReadBytesAsync(Constants.AcknowledgedBuffer.Length).ConfigureAwait(false);

                if(ackBuffer != Constants.AcknowledgedBuffer)
                {
                    throw new IOException("Unexpected acknowledgement from sender");
                }
            }
            catch(Exception)
            {
                transaction.Rollback();
                throw;
            }
        }

        public async Task ActualCommit(Stream stream, IIncomingTransaction transaction)
        {
            try
            {
                transaction.Commit();
            }
            catch(Exception)
            {
                await SendBuffer(stream, Constants.RevertBuffer);
                throw;
            }
        }
    }
}