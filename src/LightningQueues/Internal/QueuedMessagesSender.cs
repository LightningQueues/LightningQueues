using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Storage;
#pragma warning disable 420
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace LightningQueues.Internal
{
    public class QueuedMessagesSender
    {
        private readonly QueueStorage _queueStorage;
    	private readonly IQueueManager _queueManager;
        private readonly ILogger _logger;
        private volatile bool _continueSending = true;
        private volatile int _currentlySendingCount;
        private volatile int _currentlyConnecting;
		private readonly object _lock = new object();

        public QueuedMessagesSender(QueueStorage queueStorage, IQueueManager queueManager, ILogger logger)
        {
        	_queueStorage = queueStorage;
        	_queueManager = queueManager;
            _logger = logger;
        }

        public int CurrentlySendingCount
        {
            get { return _currentlySendingCount; }
        }

        public int CurrentlyConnectingCount
        {
            get { return _currentlyConnecting; }
        }

        public void Send()
        {
            while (_continueSending)
            {
                IList<PersistentMessage> messages = null;

                //normal conditions will be at 5, when there are several unreliable endpoints 
                //it will grow up to 31 connections all attempting to connect, timeouts can take up to 30 seconds
            	if ((_currentlySendingCount - _currentlyConnecting > 5) || _currentlyConnecting > 30)
                {
					lock (_lock)
						Monitor.Wait(_lock, TimeSpan.FromSeconds(1));
                    continue;
                }

                Endpoint point = null;
                _queueStorage.Send(actions =>
                {
                    messages = actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024*1024, out point);

                    actions.Commit();
                });

                if (messages.Count == 0)
                {
					lock (_lock)
						Monitor.Wait(_lock, TimeSpan.FromSeconds(1));
                    continue;
                }

                Interlocked.Increment(ref _currentlySendingCount);
                Interlocked.Increment(ref _currentlyConnecting);

                new Sender(_logger)
                {
                    Connected = () => Interlocked.Decrement(ref _currentlyConnecting),
                    Destination = point,
                    Messages = messages.ToArray(),
					Success = OnSuccess(messages),
					Failure = OnFailure(point, messages),
                    FailureToConnect = e =>
                    {
                        Interlocked.Decrement(ref _currentlyConnecting);
                        OnFailure(point, messages)(e);
                    },
					Revert = OnRevert(point),
                    Commit = OnCommit(point, messages)
                }.Send();
            }
        }

        private Action<MessageBookmark[]> OnRevert(Endpoint endpoint	)
        {
        	return bookmarksToRevert =>
			{
				_queueStorage.Send(actions =>
				{
					actions.RevertBackToSend(bookmarksToRevert);

					actions.Commit();
				});
				_queueManager.FailedToSendTo(endpoint);
			};
        }

        private Action<Exception> OnFailure(Endpoint endpoint, IEnumerable<PersistentMessage> messages)
        {
            return exception =>
            {
                try
                {
                    _queueStorage.Send(actions =>
                        {
                            foreach (var message in messages)
                            {
                                actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark,
                                                                                exception is QueueDoesNotExistsException);
                            }

                            actions.Commit();
                            _queueManager.FailedToSendTo(endpoint);
                        });
                }
                finally
                {
                    Interlocked.Decrement(ref _currentlySendingCount);
                }
            };
        }

        private Func<MessageBookmark[]> OnSuccess(IEnumerable<PersistentMessage> messages)
        {
            return () =>
            {
                try
                {
                    var newBookmarks = new List<MessageBookmark>();
                    _queueStorage.Send(actions =>
                    {
                        foreach (var message in messages)
                        {
                            var bookmark = actions.MarkOutgoingMessageAsSuccessfullySent(message.Bookmark);
                            newBookmarks.Add(bookmark);
                        }

                        actions.Commit();
                    });
                    return newBookmarks.ToArray();
                }
                finally
                {
                    Interlocked.Decrement(ref _currentlySendingCount);
                }
            };
        }

        private Action OnCommit(Endpoint endpoint, IEnumerable<PersistentMessage> messages)
        {
            return () =>
            {
                foreach (var message in messages)
                {
                    _queueManager.OnMessageSent(new MessageEventArgs(endpoint, message));
                }
            };
        }

        public void Stop()
        {
            _continueSending = false;
            while (_currentlySendingCount > 0)
                Thread.Sleep(TimeSpan.FromSeconds(1));
			lock(_lock)
				Monitor.Pulse(_lock);
        }
    }
}
#pragma warning restore 420