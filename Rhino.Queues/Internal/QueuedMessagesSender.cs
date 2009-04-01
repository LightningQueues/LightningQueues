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
        private readonly QueueFactory queueFactory;
        private volatile bool continueSending = true;
        private volatile int currentlySendingCount;

        public QueuedMessagesSender(QueueFactory queueFactory)
        {
            this.queueFactory = queueFactory;
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
                queueFactory.Send(actions =>
                {
                    messages = actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024*1024, out point);

                    actions.Commit();
                });

                if (messages.Count == 0)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    continue;
                }

                Interlocked.Increment(ref currentlySendingCount);

                new Sender
                {
                    Destination = point,
                    Messages = messages.ToArray(),
                    Success = OnSuccess(messages),
                    Failure = OnFailure(messages),
                    Revert = OnRevert
                }.Send();
            }
        }

        private void OnRevert(MessageBookmark[] bookmarksToRevert)
        {
            queueFactory.Send(actions =>
            {
                actions.RevertBackToSend(bookmarksToRevert);

                actions.Commit();
            });
        }

        private Action<Exception> OnFailure(IEnumerable<PersistentMessage> messages)
        {
            return exception => queueFactory.Send(actions =>
            {
                foreach (var message in messages)
                {
                    actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark,
                                                                    exception is QueueDoesNotExistsException);
                }

                actions.Commit();
                Interlocked.Decrement(ref currentlySendingCount);
            });
        }

        private Func<MessageBookmark[]> OnSuccess(IEnumerable<PersistentMessage> messages)
        {
            return () =>
            {
                var newBookmarks = new List<MessageBookmark>();
                queueFactory.Send(actions =>
                {
                    foreach (var message in messages)
                    {
                        var bookmark = actions.MarkOutgoingMessageAsSuccessfullySent(message.Bookmark);
                        newBookmarks.Add(bookmark);
                    }

                    actions.Commit();
                    Interlocked.Decrement(ref currentlySendingCount);
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