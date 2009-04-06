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
    	private readonly QueueManager queueManager;
    	private volatile bool continueSending = true;
        private volatile int currentlySendingCount;

        public QueuedMessagesSender(QueueStorage queueStorage, QueueManager queueManager)
        {
        	this.queueStorage = queueStorage;
        	this.queueManager = queueManager;
        }

    	public void Send()
        {
            while (continueSending)
            {
                IList<PersistentMessage> messages = null;

                var count = currentlySendingCount;
                if (count > 5)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
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
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    continue;
                }

#pragma warning disable 420
                Interlocked.Increment(ref currentlySendingCount);
#pragma warning restore 420

                new Sender
                {
                    Destination = point,
                    Messages = messages.ToArray(),
					Success = OnSuccess(messages),
					Failure = OnFailure(point, messages),
					Revert = OnRevert(point)
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
            return exception => queueStorage.Send(actions =>
            {
                foreach (var message in messages)
                {
                    actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark,
                                                                    exception is QueueDoesNotExistsException);
                }

                actions.Commit();
#pragma warning disable 420
                Interlocked.Decrement(ref currentlySendingCount);
#pragma warning restore 420
				queueManager.FailedToSendTo(endpoint);
            });
        }

        private Func<MessageBookmark[]> OnSuccess(IEnumerable<PersistentMessage> messages)
        {
            return () =>
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
#pragma warning disable 420
                    Interlocked.Decrement(ref currentlySendingCount);
#pragma warning restore 420
                });
                return newBookmarks.ToArray();
            };
        }

        public void Stop()
        {
            continueSending = false;
            while (currentlySendingCount > 0)
                Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }
}