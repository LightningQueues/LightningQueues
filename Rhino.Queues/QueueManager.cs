using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Transactions;
using log4net;
using Rhino.Queues.Internal;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;
using System.Linq;

namespace Rhino.Queues
{
    public class QueueManager : IDisposable
    {
        [ThreadStatic]
        private static TransactionEnlistment enlistment;

        [ThreadStatic]
        private static Transaction currentlyEnslistedTransaction;

        private volatile bool wasDisposed;
        private volatile int currentlyInCriticalReceiveStatus;
        private readonly IPEndPoint endpoint;
        private readonly object newMessageArrivedLock = new object();
        private readonly string path;
        private readonly QueueFactory queueFactory;
        private readonly Reciever reciever;
        private readonly Thread sendingThread;
        private readonly QueuedMessagesSender queuedMessagesSender;
        private readonly ILog logger = LogManager.GetLogger(typeof (QueueManager));

        public QueueManager(IPEndPoint endpoint, string path)
        {
            this.endpoint = endpoint;
            this.path = path;
            queueFactory = new QueueFactory(path);
            queueFactory.Initialize();

            reciever = new Reciever(endpoint, AcceptMessages);
            reciever.Start();

            HandleRecovery();

            queuedMessagesSender = new QueuedMessagesSender(queueFactory);
            sendingThread = new Thread(queuedMessagesSender.Send)
            {
                IsBackground = true
            };
            sendingThread.Start();
        }

        private void HandleRecovery()
        {
            queueFactory.Global(actions =>
            {
                actions.MarkAllOutgoingInFlightMessagesAsReadyToSend();
                actions.MarkAllProcessedMessagesWithTransactionsNotRegisterForRecoveryAsReadyToDeliver();
                foreach (var bytes in actions.GetRecoveryInformation())
                {
                    TransactionManager.Reenlist(queueFactory.Id, bytes,
                        new TransactionEnlistment(queueFactory, () => { }));
                }
                actions.Commit();
            });

            TransactionManager.RecoveryComplete(queueFactory.Id);
        }

        public string Path
        {
            get { return path; }
        }

        public IPEndPoint Endpoint
        {
            get { return endpoint; }
        }

        #region IDisposable Members

        public void Dispose()
        {
            wasDisposed = true;

            lock (newMessageArrivedLock)
            {
                Monitor.PulseAll(newMessageArrivedLock);
            }
            queuedMessagesSender.Stop();
            sendingThread.Join();

            reciever.Dispose();

            while (currentlyInCriticalReceiveStatus > 0)
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }

            queueFactory.Dispose();
        }

        #endregion

        private void AssertNotDisposed()
        {
            if(wasDisposed)
                throw new ObjectDisposedException("QueueManager");
        }

        public IQueue GetQueue(string queue)
        {
            return new Queue(this, queue);
        }

        public PersistentMessage[] GetAllMessages(string queueName, string subqueue)
        {
            AssertNotDisposed();
            PersistentMessage[] messages = null;
            queueFactory.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllMessages(subqueue).ToArray();
                actions.Commit();
            });
            return messages;
        }

        public HistoryMessage[] GetAllProcessedMessages(string queueName, string subqueue)
        {
            AssertNotDisposed(); 
            HistoryMessage[] messages = null;
            queueFactory.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllProcessedMessages(subqueue).ToArray();
                actions.Commit();
            });
            return messages;
        }

        public PersistentMessageToSend[] GetAllSentMessages()
        {
            AssertNotDisposed(); 
            PersistentMessageToSend[] msgs = null;
            queueFactory.Send(actions =>
            {
                msgs = actions.GetSentMessages().ToArray();

                actions.Commit();
            });
            return msgs;
        }

        public PersistentMessageToSend[] GetMessagesCurrentlySending()
        {
            AssertNotDisposed();
            PersistentMessageToSend[] msgs = null;
            queueFactory.Send(actions =>
            {
                msgs = actions.GetMessagesToSend().ToArray();

                actions.Commit();
            });
            return msgs;
        }

        public Message Peek(string queueName)
        {
            return Peek(queueName, null, TimeSpan.FromDays(1));
        }

        public Message Peek(string queueName, TimeSpan timeout)
        {
            return Peek(queueName, null, timeout);
        }

        public Message Peek(string queueName, string subqueue)
        {
            return Peek(queueName, subqueue, TimeSpan.FromDays(1));
        }

        public Message Peek(string queueName, string subqueue, TimeSpan timeout)
        {
            var remaining = timeout;
            while (true)
            {
                var message = PeekMessageFromQueue(queueName, subqueue);
                if (message != null)
                    return message;

                lock (newMessageArrivedLock)
                {
                    message = PeekMessageFromQueue(queueName, subqueue);
                    if (message != null)
                        return message;

                    var sp = Stopwatch.StartNew();
                    if (Monitor.Wait(newMessageArrivedLock, remaining) == false)
                        throw new TimeoutException("No message arrived in the specified timeframe " + timeout);
                    remaining = remaining - sp.Elapsed;
                }
            }
        }

        public Message Receive(string queueName)
        {
            return Receive(queueName, null, TimeSpan.FromDays(1));
        }

        public Message Receive(string queueName, TimeSpan timeout)
        {
            return Receive(queueName, null, timeout);
        }

        public Message Receive(string queueName, string subqueue)
        {
            return Receive(queueName, subqueue, TimeSpan.FromDays(1));
        }

        public Message Receive(string queueName, string subqueue, TimeSpan timeout)
        {
            EnsureEnslistment();

            var remaining = timeout;
            while (true)
            {
                var message = GetMessageFromQueue(queueName, subqueue);
                if (message != null)
                    return message;

                lock (newMessageArrivedLock)
                {
                    message = GetMessageFromQueue(queueName, subqueue);
                    if (message != null)
                        return message;

                    var sp = Stopwatch.StartNew();
                    if (Monitor.Wait(newMessageArrivedLock, remaining) == false)
                        throw new TimeoutException("No message arrived in the specified timeframe " + timeout);
                    remaining = remaining - sp.Elapsed;
                }
            }
        }

        public MessageId Send(Uri uri, MessagePayload payload)
        {
            EnsureEnslistment();

            var parts = uri.AbsolutePath.Substring(1).Split('/');
            var queue = parts[0];
            string subqueue = null;
            if (parts.Length > 1)
            {
                subqueue = string.Join("/", parts.Skip(1).ToArray());
            }

            int msgId = 0;
            queueFactory.Global(actions =>
            {
                var port = uri.Port;
                if (port == -1)
                    port = 2200;
                msgId = actions.RegisterToSend(new Endpoint(uri.Host, port), queue,
                                               subqueue, payload, enlistment.Id);

                actions.Commit();
            });
            return new MessageId
            {
                Guid = queueFactory.Id,
                Number = msgId
            };
        }

        private void EnsureEnslistment()
        {
            AssertNotDisposed(); 
            
            if (Transaction.Current == null)
                throw new InvalidOperationException("You must use TransactionScope when using Rhino.Queues");

            if (currentlyEnslistedTransaction == Transaction.Current)
                return;
            // need to change the enslitment

            enlistment = new TransactionEnlistment(queueFactory, () =>
            {
                lock (newMessageArrivedLock)
                {
                    Monitor.PulseAll(newMessageArrivedLock);
                }
            });
            currentlyEnslistedTransaction = Transaction.Current;
        }

        private PersistentMessage GetMessageFromQueue(string queueName, string subqueue)
        {
            AssertNotDisposed(); 
            PersistentMessage message = null;
            queueFactory.Global(actions =>
            {
                message = actions.GetQueue(queueName).Dequeue(subqueue);

                if (message != null)
                {
                    actions.RegisterUpdateToReverse(
                        enlistment.Id,
                        message.Bookmark,
                        MessageStatus.ReadyToDeliver,
                        subqueue);
                }

                actions.Commit();
            });
            return message;
        }

        private PersistentMessage PeekMessageFromQueue(string queueName, string subqueue)
        {
            AssertNotDisposed();
            PersistentMessage message = null;
            queueFactory.Global(actions =>
            {
                message = actions.GetQueue(queueName).Peek(subqueue);

                actions.Commit();
            });
            if (message != null)
            {
                logger.DebugFormat("Peeked message with id '{0}' from '{1}/{2}'",
                                   message.Id, queueName, subqueue);
            }
            return message;
        }

        private IMessageAcceptance AcceptMessages(Message[] msgs)
        {
            var bookmarks = new List<MessageBookmark>();
            queueFactory.Global(actions =>
            {
                foreach (var msg in msgs)
                {
                    var bookmark = actions.GetQueue(msg.Queue).Enqueue(msg);
                    bookmarks.Add(bookmark);
                }
                actions.Commit();
            });

            return new MessageAcceptance(this, bookmarks, queueFactory);
        }

        #region Nested type: MessageAcceptance

        private class MessageAcceptance : IMessageAcceptance
        {
            private readonly IList<MessageBookmark> bookmarks;
            private readonly QueueManager parent;
            private readonly QueueFactory queueFactory;

            public MessageAcceptance(QueueManager parent, IList<MessageBookmark> bookmarks, QueueFactory queueFactory)
            {
                this.parent = parent;
                this.bookmarks = bookmarks;
                this.queueFactory = queueFactory;
#pragma warning disable 420
                Interlocked.Increment(ref parent.currentlyInCriticalReceiveStatus);
#pragma warning restore 420
            }

            #region IMessageAcceptance Members

            public void Commit()
            {
                try
                {
                    queueFactory.Global(actions =>
                    {
                        foreach (var bookmark in bookmarks)
                        {
                            actions.GetQueue(bookmark.QueueName)
                                .SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                        }
                        actions.Commit();
                    });

                    lock (parent.newMessageArrivedLock)
                    {
                        Monitor.PulseAll(parent.newMessageArrivedLock);
                    }
                }
                finally
                {
#pragma warning disable 420
                    Interlocked.Decrement(ref parent.currentlyInCriticalReceiveStatus);
#pragma warning restore 420

                }
            }

            public void Abort()
            {
                try
                {
                    queueFactory.Global(actions =>
                    {
                        foreach (var bookmark in bookmarks)
                        {
                            actions.GetQueue(bookmark.QueueName)
                                .Discard(bookmark);
                        }
                        actions.Commit();
                    });
                }
                finally
                {
#pragma warning disable 420
                    Interlocked.Decrement(ref parent.currentlyInCriticalReceiveStatus);
#pragma warning restore 420
                }
            }

            #endregion
        }

        #endregion

        public void CreateQueues(params string[] queueNames)
        {
            AssertNotDisposed();

            queueFactory.Global(actions =>
            {
                foreach (var queueName in queueNames)
                {
                    actions.CreateQueueIfDoesNotExists(queueName);
                }

                actions.Commit();
            });
        }

        public string[] Queues
        {
            get
            {
                AssertNotDisposed();
                string[] queues = null;
                queueFactory.Global(actions =>
                {
                    queues = actions.GetAllQueuesNames();

                    actions.Commit();
                });
                return queues;
            }
        }

        public void MoveTo(string subqueue, Message message)
        {
            AssertNotDisposed();
            EnsureEnslistment();
            
            queueFactory.Global(actions =>
            {
                var queue = actions.GetQueue(message.Queue);
                var bookmark = queue.MoveTo(subqueue, (PersistentMessage)message);
                actions.RegisterUpdateToReverse(enlistment.Id,
                    bookmark, MessageStatus.ReadyToDeliver,
                    message.SubQueue
                    );
                actions.Commit();
            });
        }

        public void EnqueueDirectlyTo(string queue, string subqueue, MessagePayload payload)
        {
            EnsureEnslistment();

            queueFactory.Global(actions =>
            {
                var queueActions = actions.GetQueue(queue);

                var bookmark = queueActions.Enqueue(new PersistentMessage
                {
                    Data = payload.Data,
                    Headers = payload.Headers,
                    Id = new MessageId
                    {
                        Guid = queueFactory.Id,
                        Number = -1
                    },
                    Queue = queue,
                    SentAt = DateTime.Now,
                    SubQueue = subqueue,
                    Status = MessageStatus.EnqueueWait
                });
                actions.RegisterUpdateToReverse(enlistment.Id, bookmark, MessageStatus.EnqueueWait, subqueue);

                actions.Commit();
            });
            lock(newMessageArrivedLock)
            {
                Monitor.PulseAll(newMessageArrivedLock);
            }
        }

        public PersistentMessage PeekById(string queueName, MessageId id)
        {
            PersistentMessage message = null;
            queueFactory.Global(actions =>
            {
                var queue = actions.GetQueue(queueName);

                message = queue.PeekById(id);

                actions.Commit();
            });
            return message;
        }
    }
}