using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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

        private readonly ILog logger = LogManager.GetLogger(typeof (QueueManager));
        private volatile int currentlyInCriticalReceiveStatus;
        private readonly IPEndPoint endpoint;
        private readonly object newMessageArrivedLock = new object();
        private readonly string path;
        private readonly QueueFactory queueFactory;
        private readonly Reciever reciever;
        private readonly Thread sendingThread;
        private QueuedMessagesSender queuedMessagesSender;

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

        public PersistentMessage[] GetAllMessages(string queueName)
        {
            PersistentMessage[] messages = null;
            queueFactory.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllMessages().ToArray();
                actions.Commit();
            });
            return messages;
        }

        public HistoryMessage[] GetAllProcessedMessages(string queueName)
        {
            HistoryMessage[] messages = null;
            queueFactory.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllProcessedMessages().ToArray();
                actions.Commit();
            });
            return messages;
        }

        public PersistentMessageToSend[] GetAllSentMessages()
        {
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
            PersistentMessageToSend[] msgs = null;
            queueFactory.Send(actions =>
            {
                msgs = actions.GetMessagesToSend().ToArray();

                actions.Commit();
            });
            return msgs;
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

        public void Send(Uri uri, byte[] msgBytes)
        {
            var parts = uri.AbsolutePath.Substring(1).Split('/');
            var queue = parts[0];
            string subQueue = null;
            if(parts.Length > 1)
            {
                subQueue = string.Join("/", parts.Skip(1).ToArray());
            }

            EnsureEnslistment();

            queueFactory.Global(actions =>
            {
                actions.RegisterToSend(new Endpoint(uri.Host, uri.Port), queue, subQueue, msgBytes, enlistment.Id);

                actions.Commit();
            });
        }

        private void EnsureEnslistment()
        {
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
                Interlocked.Increment(ref parent.currentlyInCriticalReceiveStatus);
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
                    Interlocked.Decrement(ref parent.currentlyInCriticalReceiveStatus);
                    
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
                    Interlocked.Decrement(ref parent.currentlyInCriticalReceiveStatus);
                }
            }

            #endregion
        }

        #endregion

        public void CreateQueues(params string[] queueNames)
        {
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
                string[] queues = null;
                queueFactory.Global(actions =>
                {
                    queues = actions.GetAllQueuesNames();

                    actions.Commit();
                });
                return queues;
            }
        }

        public void MoveTo(string subQueue, Message message)
        {
            queueFactory.Global(actions =>
            {
                var queue = actions.GetQueue(message.Queue);
                var bookmark = queue.MoveTo(subQueue, (PersistentMessage)message);
                actions.RegisterUpdateToReverse(enlistment.Id,
                    bookmark, MessageStatus.ReadyToDeliver,
                    message.SubQueue
                    );
                actions.Commit();
            });
        }
    }
}