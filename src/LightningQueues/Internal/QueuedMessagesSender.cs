using System.Linq;
using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Storage;
using System;
using System.Collections.Generic;

namespace LightningQueues.Internal
{
    public class QueuedMessagesSender
    {
        private readonly QueueStorage _queueStorage;
        private readonly ILogger _logger;
        private readonly SendingChoke _choke;
        private volatile bool _continueSending = true;

        public QueuedMessagesSender(QueueStorage queueStorage, SendingChoke choke, ILogger logger)
        {
        	_queueStorage = queueStorage;
            _logger = logger;
            _choke = choke;
        }

        public void Send()
        {
            while (_continueSending)
            {
                if(!_choke.ShouldBeginSend())
                    continue;

                var messages = gatherMessagesToSend();

                if (messages == null)
                {
                    _choke.NoMessagesToSend();
                    continue;
                }

                _choke.StartSend();

                SendMessages(messages.Destination, messages.Messages);
            }
        }

        public async void SendMessages(Endpoint destination, PersistentMessage[] messages)
        {
            var sender = createSender(destination, messages);
            MessageBookmark[] sendHistoryBookmarks = null;
            sender.Success = () => sendHistoryBookmarks = success(messages);
            try
            {
                await sender.Send();
                _logger.DebugMessage(() => new MessagesSent(messages, destination));
            }
            catch (FailedToConnectException ex)
            {
                _logger.InfoMessage(new FailedToSend(destination, "Failed to connect", ex));
                failedToConnect(messages);
            }
            catch (QueueDoesNotExistsException)
            {
                _logger.InfoMessage(new FailedToSend(destination, "Queue doesn't exist"));
                failedToSend(messages, true);
            }
            catch (RevertSendException)
            {
                _logger.InfoMessage(new FailedToSend(destination, "Revert was received"));
                revert(sendHistoryBookmarks, messages);
            }
            catch (Exception ex)
            {
                _logger.InfoMessage(new FailedToSend(destination, "Exception was thrown", ex));
                failedToSend(messages);
            }
        }

        private Sender createSender(Endpoint destination, PersistentMessage[] messages)
        {
            return new Sender(_logger)
            {
                Connected = () => _choke.SuccessfullyConnected(),
                Destination = destination,
                Messages = messages,
            };
        }

        private MessagesForEndpoint gatherMessagesToSend()
        {
            return _queueStorage.Send(actions => actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024 * 1024));
        }

        private void failedToConnect(IEnumerable<PersistentMessage> messages)
        {
            _choke.FailedToConnect();
            failedToSend(messages);
        }

        private void failedToSend(IEnumerable<PersistentMessage> messages, bool queueDoesntExist = false)
        {
            try
            {
                _queueStorage.Send(actions =>
                {
                    foreach (var message in messages)
                    {
                        actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark, queueDoesntExist);
                    }
                });
            }
            finally
            {
                _choke.FinishedSend();
            }
        }

        private void revert(MessageBookmark[] sendHistoryBookmarks, IEnumerable<PersistentMessage> messages)
        {
            _queueStorage.Send(actions => actions.RevertBackToSend(sendHistoryBookmarks));
            failedToSend(messages);
        }

        private MessageBookmark[] success(IEnumerable<PersistentMessage> messages)
        {
            try
            {
                var newBookmarks = _queueStorage.Send(actions => 
                    messages.Select(message => actions.MarkOutgoingMessageAsSuccessfullySent(message.Bookmark)).ToArray());
                return newBookmarks;
            }
            finally
            {
                _choke.FinishedSend();
            }
        }

        public void Stop()
        {
            _continueSending = false;
            _choke.StopSending();
        }
    }
}