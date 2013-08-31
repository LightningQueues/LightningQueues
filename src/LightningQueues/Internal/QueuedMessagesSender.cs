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
    	private readonly IQueueManager _queueManager;
        private readonly ILogger _logger;
        private readonly SendingChoke _choke;
        private volatile bool _continueSending = true;

        public QueuedMessagesSender(QueueStorage queueStorage, SendingChoke choke, IQueueManager queueManager, ILogger logger)
        {
        	_queueStorage = queueStorage;
        	_queueManager = queueManager;
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
                _logger.Info("Failed to connect to {0} because {1}", destination, ex);
                failedToConnect(destination, messages);
            }
            catch (QueueDoesNotExistsException)
            {
                failedToSend(destination, messages, true);
            }
            catch (RevertSendException)
            {
                revert(destination, sendHistoryBookmarks);
            }
            catch (Exception)
            {
                failedToConnect(destination, messages);
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
            MessagesForEndpoint messages = null;
            _queueStorage.Send(actions =>
            {
                messages = actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024 * 1024);
                actions.Commit();
            });
            return messages;
        }

        private void failedToConnect(Endpoint endpoint, IEnumerable<PersistentMessage> messages)
        {
            _choke.FailedToConnect();
            failedToSend(endpoint, messages);
        }

        private void failedToSend(Endpoint endpoint, IEnumerable<PersistentMessage> messages, bool queueDoesntExist = false)
        {
            try
            {
                _queueStorage.Send(actions =>
                {
                    foreach (var message in messages)
                    {
                        actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark, queueDoesntExist);
                    }

                    actions.Commit();
                    _queueManager.FailedToSendTo(endpoint);
                });
            }
            finally
            {
                _choke.FinishedSend();
            }
        }

        private void revert(Endpoint endpoint, MessageBookmark[] sendHistoryBookmarks)
        {
            _logger.Info("Got back revert message from receiver {0}, reverting send", endpoint);
            _queueStorage.Send(actions =>
            {
                actions.RevertBackToSend(sendHistoryBookmarks);
                actions.Commit();
            });
            _queueManager.FailedToSendTo(endpoint);
        }

        private MessageBookmark[] success(IEnumerable<PersistentMessage> messages)
        {
            try
            {
                var newBookmarks = new List<MessageBookmark>();
                _queueStorage.Send(actions =>
                {
                    var result = messages.Select(message => actions.MarkOutgoingMessageAsSuccessfullySent(message.Bookmark));
                    newBookmarks.AddRange(result);
                    actions.Commit();
                });
                return newBookmarks.ToArray();
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