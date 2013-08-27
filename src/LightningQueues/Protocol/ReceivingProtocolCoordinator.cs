using System;
using System.IO;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol.Chunks;

namespace LightningQueues.Protocol
{
    public class ReceivingProtocolCoordinator
    {
        private readonly ILogger _logger;

        public ReceivingProtocolCoordinator(ILogger logger)
        {
            _logger = logger;
        }

        public async Task ReadMessagesAsync(string endpoint, Stream stream,
                                            Func<Message[], IMessageAcceptance> acceptMessages)
        {
            bool serializationErrorOccurred = false;
            Message[] messages = null;
            try
            {
                var buffer = await new ReadLength(_logger, endpoint).GetAsync(stream);
                messages = await new ReadMessage(_logger, buffer, endpoint).GetAsync(stream);
            }
            catch (SerializationException exception)
            {
                _logger.Info("Unable to deserialize messages", exception);
                serializationErrorOccurred = true;
            }

            if (serializationErrorOccurred)
            {
                await new WriteSerializationError(_logger).ProcessAsync(stream);
            }

            IMessageAcceptance acceptance = null;
            byte[] errorBytes = null;
            try
            {
                acceptance = acceptMessages(messages);
                _logger.Debug("All messages from {0} were accepted", endpoint);
            }
            catch (QueueDoesNotExistsException)
            {
                _logger.Info("Failed to accept messages from {0} because queue does not exists", endpoint);
                errorBytes = ProtocolConstants.QueueDoesNoExiststBuffer;
            }
            catch (Exception exception)
            {
                errorBytes = ProtocolConstants.ProcessingFailureBuffer;
                _logger.Info("Failed to accept messages from " + endpoint, exception);
            }

            if (errorBytes != null)
            {
                await new WriteProcessingError(_logger, errorBytes, endpoint).ProcessAsync(stream);
                return;
            }

            try
            {
                await new WriteReceived(_logger, endpoint).ProcessAsync(stream);
            }
            catch (Exception)
            {
                acceptance.Abort();
                return;
            }

            try
            {
                await new ReadAcknowledgement(_logger, endpoint).ProcessAsync(stream);
            }
            catch (Exception)
            {
                acceptance.Abort();
                return;
            }

            bool commitSuccessful;
            try
            {
                acceptance.Commit();
                commitSuccessful = true;
            }
            catch (Exception exception)
            {
                _logger.Info("Unable to commit messages from " + endpoint, exception);
                commitSuccessful = false;
            }

            if (commitSuccessful == false)
            {
                await new WriteRevert(_logger, endpoint).ProcessAsync(stream);
            }
        }
    }
}