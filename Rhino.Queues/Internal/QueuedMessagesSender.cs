#pragma warning disable 420
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Rhino.Queues.Exceptions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;

namespace Rhino.Queues.Internal
{
    public class QueuedMessagesSender
    {
        private readonly QueueStorage queueStorage;
    	private readonly IQueueManager queueManager;
    	private volatile bool continueSending = true;
        private volatile int currentlySendingCount;
        private volatile int currentlyConnecting;
		private object @lock = new object();

        public QueuedMessagesSender(QueueStorage queueStorage, IQueueManager queueManager)
        {
        	this.queueStorage = queueStorage;
        	this.queueManager = queueManager;
        }

        public int CurrentlySendingCount
        {
            get { return currentlySendingCount; }
        }

        public int CurrentlyConnectingCount
        {
            get { return currentlyConnecting; }
        }

        public void Send()
        {
            while (continueSending)
            {
                IList<PersistentMessage> messages = null;

                //normal conditions will be at 5, when there are several unreliable endpoints 
                //it will grow up to 31 connections all attempting to connect, timeouts can take up to 30 seconds
            	if ((currentlySendingCount - currentlyConnecting > 5) || currentlyConnecting > 30)
                {
					lock (@lock)
						Monitor.Wait(@lock, TimeSpan.FromSeconds(1));
                    continue;
                }

                Endpoint point = null;
                queueStorage.Send(actions =>
                {
                    messages = actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024*1024, out point);

                    actions.Commit();
                });

                if (messages.Count == 0)
                {
					lock (@lock)
						Monitor.Wait(@lock, TimeSpan.FromSeconds(1));
                    continue;
                }

                Interlocked.Increment(ref currentlySendingCount);
                Interlocked.Increment(ref currentlyConnecting);

                new Sender
                {
                    Connected = () => Interlocked.Decrement(ref currentlyConnecting),
                    Destination = point,
                    Messages = messages.ToArray(),
					Success = OnSuccess(messages),
					Failure = OnFailure(point, messages),
                    FailureToConnect = e =>
                    {
                        Interlocked.Decrement(ref currentlyConnecting);
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
				queueStorage.Send(actions =>
				{
					actions.RevertBackToSend(bookmarksToRevert);

					actions.Commit();
				});
				queueManager.FailedToSendTo(endpoint);
			};
        }

        private Action<Exception> OnFailure(Endpoint endpoint, IEnumerable<PersistentMessage> messages)
        {
            return exception =>
            {
                try
                {
                    queueStorage.Send(actions =>
                        {
                            foreach (var message in messages)
                            {
                                actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark,
                                                                                exception is QueueDoesNotExistsException);
                            }

                            actions.Commit();
                            queueManager.FailedToSendTo(endpoint);
                        });
                }
                finally
                {
                    Interlocked.Decrement(ref currentlySendingCount);
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
                    queueStorage.Send(actions =>
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
                    Interlocked.Decrement(ref currentlySendingCount);
                }
            };
        }

        private Action OnCommit(Endpoint endpoint, IEnumerable<PersistentMessage> messages)
        {
            return () =>
            {
                foreach (var message in messages)
                {
                    queueManager.OnMessageSent(new MessageEventArgs(endpoint, message));
                }
            };
        }

        public void Stop()
        {
            continueSending = false;
            while (currentlySendingCount > 0)
                Thread.Sleep(TimeSpan.FromSeconds(1));
			lock(@lock)
				Monitor.Pulse(@lock);
        }
    }
}
#pragma warning restore 420