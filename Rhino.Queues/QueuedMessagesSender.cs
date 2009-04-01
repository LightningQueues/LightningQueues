using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Rhino.Queues.Exceptions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;
using System.Linq;

namespace Rhino.Queues
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
                if(count > 5)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    continue;
                }

                Endpoint point = null;
                queueFactory.Send(actions =>
                {
                    messages = actions.GetMessagesToSendAndMarkThemAsInFlight(100, 1024 * 1024, out point);

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
                    Failure = OnFailure(messages)
                }.Send();
            }
        }

        private Action<Exception> OnFailure(IEnumerable<PersistentMessage> messages)
        {
            return exception => queueFactory.Send(actions =>
            {
                foreach (var message in messages)
                {
                    actions.MarkOutgoingMessageAsFailedTransmission(message.Bookmark, exception is QueueDoesNotExistsException);
                }

                actions.Commit();
                Interlocked.Decrement(ref currentlySendingCount);
            });
        }

        private Action OnSuccess(IEnumerable<PersistentMessage> messages)
        {
            return () => queueFactory.Send(actions =>
            {
                foreach (var message in messages)
                {
                    actions.MarkOutgoingMessageAsSuccessfullySent(message.Bookmark);
                }

                actions.Commit();
                Interlocked.Decrement(ref currentlySendingCount);
            });
        }

        public void Stop()
        {
            continueSending = false;
            while(currentlySendingCount > 0)
                Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }
}