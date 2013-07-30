using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Transactions;
using Common.Logging;
using Rhino.Queues.Internal;
using Rhino.Queues.Model;
using Rhino.Queues.Monitoring;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;
using System.Linq;

#pragma warning disable 420
namespace Rhino.Queues
{
    using Exceptions;
    using Utils;

    public class QueueManager : IQueueManager, ITransactionalQueueManager
    {
        [ThreadStatic]
        private static TransactionEnlistment Enlistment;

        [ThreadStatic]
        private static Transaction CurrentlyEnslistedTransaction;

        private volatile bool wasStarted;
        private volatile bool wasDisposed;
        private volatile bool enableEndpointPortAutoSelection;
        private volatile int currentlyInCriticalReceiveStatus;
        private volatile int currentlyInsideTransaction;
        private readonly IPEndPoint endpoint;
        private readonly object newMessageArrivedLock = new object();
        private readonly string path;
        private Timer purgeOldDataTimer;
        private readonly QueueStorage queueStorage;

        private Receiver receiver;
        private Thread sendingThread;
        private QueuedMessagesSender queuedMessagesSender;
        private readonly ILog logger = LogManager.GetLogger(typeof(QueueManager));
        private PerformanceMonitor monitor;
        private volatile bool waitingForAllMessagesToBeSent;


        private readonly ThreadSafeSet<MessageId> receivedMsgs = new ThreadSafeSet<MessageId>();
        private bool disposing;

        public QueueManagerConfiguration Configuration { get; set; }

        public int CurrentlySendingCount
        {
            get { return queuedMessagesSender.CurrentlySendingCount; }
        }

        public int CurrentlyConnectingCount
        {
            get { return queuedMessagesSender.CurrentlyConnectingCount; }
        }

        public event Action<Endpoint> FailedToSendMessagesTo;

        public event Action<object, MessageEventArgs> MessageQueuedForSend;

        public event Action<object, MessageEventArgs> MessageSent;
        public event Action<object, MessageEventArgs> MessageQueuedForReceive;
        public event Action<object, MessageEventArgs> MessageReceived;

        public QueueManager(IPEndPoint endpoint, string path)
            : this(endpoint, path, new QueueManagerConfiguration())
        {
        }

        public QueueManager(IPEndPoint endpoint, string path, QueueManagerConfiguration configuration)
        {
            Configuration = configuration;

            this.endpoint = endpoint;
            this.path = path;
            queueStorage = new QueueStorage(path, configuration);
            queueStorage.Initialize();

            queueStorage.Global(actions =>
            {
                receivedMsgs.Add(actions.GetAlreadyReceivedMessageIds());

                actions.Commit();
            });

            HandleRecovery();
        }

        public void Start()
        {
            AssertNotDisposedOrDisposing();

            if (wasStarted)
                throw new InvalidOperationException("The Start method may not be invoked more than once.");

            receiver = new Receiver(endpoint, enableEndpointPortAutoSelection, AcceptMessages);
            receiver.Start();

            queuedMessagesSender = new QueuedMessagesSender(queueStorage, this);
            sendingThread = new Thread(queuedMessagesSender.Send)
            {
                IsBackground = true,
                Name = "Rhino Queue Sender Thread for " + path
            };
            sendingThread.Start();
            purgeOldDataTimer = new Timer(_ => PurgeOldData(), null,
                                          TimeSpan.FromMinutes(3),
                                          TimeSpan.FromMinutes(3));

            wasStarted = true;
        }

        public void PurgeOldData()
        {
            logger.Info("Starting to purge old data");
            try
            {
                PurgeProcessedMessages();
                PurgeOutgoingHistory();
                PurgeOldestReceivedMessageIds();
            }
            catch (Exception exception)
            {
                logger.Warn("Failed to purge old data from the system", exception);
            }
        }

        private void PurgeProcessedMessages()
        {
            if (!Configuration.EnableProcessedMessageHistory)
                return;

            foreach (string queue in Queues)
            {
                PurgeProcessedMessagesInQueue(queue);
            }
        }

        private void PurgeProcessedMessagesInQueue(string queue)
        {
            // To make this batchable:
            // 1: Move to the end of the history (to the newest messages) and seek 
            //    backword by NumberOfMessagesToKeepInProcessedHistory.
            // 2: Save a bookmark of the current position.
            // 3: Delete from the beginning of the table (oldest messages) in batches until 
            //    a) we reach the bookmark or b) we hit OldestMessageInProcessedHistory.
            MessageBookmark purgeLimit = null;
            int numberOfMessagesToKeep = Configuration.NumberOfMessagesToKeepInProcessedHistory;
            if (numberOfMessagesToKeep > 0)
            {
                queueStorage.Global(actions =>
                {
                    var queueActions = actions.GetQueue(queue);
                    purgeLimit = queueActions.GetMessageHistoryBookmarkAtPosition(numberOfMessagesToKeep);
                    actions.Commit();
                });

                if (purgeLimit == null)
                    return;
            }

            bool foundMessages = false;
            do
            {
                foundMessages = false;
                queueStorage.Global(actions =>
                {
                    var queueActions = actions.GetQueue(queue);
                    var messages = queueActions.GetAllProcessedMessages(batchSize: 250)
                        .TakeWhile(x => (purgeLimit == null || !x.Bookmark.Equals(purgeLimit))
                            && (DateTime.Now - x.SentAt) > Configuration.OldestMessageInProcessedHistory);

                    foreach (var message in messages)
                    {
                        foundMessages = true;
                        logger.DebugFormat("Purging message {0} from queue {1}/{2}", message.Id, message.Queue, message.SubQueue);
                        queueActions.DeleteHistoric(message.Bookmark);
                    }

                    actions.Commit();
                });
            } while (foundMessages);
        }

        private void PurgeOutgoingHistory()
        {
            // Outgoing messages are still stored in the history in case the sender 
            // needs to revert, so there will still be messages to purge even when
            // the QueueManagerConfiguration has disabled outgoing history.
            //
            // To make this batchable:
            // 1: Move to the end of the history (to the newest messages) and seek 
            //    backword by NumberOfMessagesToKeepInOutgoingHistory.
            // 2: Save a bookmark of the current position.
            // 3: Delete from the beginning of the table (oldest messages) in batches until 
            //    a) we reach the bookmark or b) we hit OldestMessageInOutgoingHistory.

            MessageBookmark purgeLimit = null;
            int numberOfMessagesToKeep = Configuration.NumberOfMessagesToKeepInOutgoingHistory;
            if (numberOfMessagesToKeep > 0 && Configuration.EnableOutgoingMessageHistory)
            {
                queueStorage.Global(actions =>
                {
                    purgeLimit = actions.GetSentMessageBookmarkAtPosition(numberOfMessagesToKeep);
                    actions.Commit();
                });

                if (purgeLimit == null)
                    return;
            }

            bool foundMessages = false;
            do
            {
                foundMessages = false;
                queueStorage.Global(actions =>
                {
                    IEnumerable<PersistentMessageToSend> sentMessages = actions.GetSentMessages(batchSize: 250)
                        .TakeWhile(x => (purgeLimit == null || !x.Bookmark.Equals(purgeLimit))
                            && (!Configuration.EnableOutgoingMessageHistory || (DateTime.Now - x.SentAt) > Configuration.OldestMessageInOutgoingHistory));

                    foreach (var sentMessage in sentMessages)
                    {
                        foundMessages = true;
                        logger.DebugFormat("Purging sent message {0} to {1}/{2}/{3}", sentMessage.Id, sentMessage.Endpoint,
                                           sentMessage.Queue, sentMessage.SubQueue);
                        actions.DeleteMessageToSendHistoric(sentMessage.Bookmark);
                    }

                    actions.Commit();
                });
            } while (foundMessages);
        }

        private void PurgeOldestReceivedMessageIds()
        {
            int totalCount = 0;
            List<MessageId> deletedMessageIds = null;
            do
            {
                queueStorage.Global(actions =>
                {
                    deletedMessageIds = actions.DeleteOldestReceivedMessageIds(
                        Configuration.NumberOfReceivedMessageIdsToKeep, numberOfItemsToDelete: 250)
                        .ToList();
                    actions.Commit();
                });
                receivedMsgs.Remove(deletedMessageIds);
                totalCount += deletedMessageIds.Count;
            } while (deletedMessageIds.Count > 0);

            logger.InfoFormat("Purged {0} message ids", totalCount);
        }

        private void HandleRecovery()
        {
            var recoveryRequired = false;
            queueStorage.Global(actions =>
            {
                actions.MarkAllOutgoingInFlightMessagesAsReadyToSend();
                actions.MarkAllProcessedMessagesWithTransactionsNotRegisterForRecoveryAsReadyToDeliver();
                foreach (var bytes in actions.GetRecoveryInformation())
                {
                    recoveryRequired = true;
                    TransactionManager.Reenlist(queueStorage.Id, bytes,
                        new TransactionEnlistment(queueStorage, () => { }, () => { }));
                }
                actions.Commit();
            });
            if (recoveryRequired)
                TransactionManager.RecoveryComplete(queueStorage.Id);
        }

        public ITransactionalScope BeginTransactionalScope()
        {
            return new TransactionalScope(this, new QueueTransaction(queueStorage, OnTransactionComplete, AssertNotDisposed));
        }

        public void EnablePerformanceCounters()
        {
            if (wasStarted)
                throw new InvalidOperationException("Performance counters cannot be enabled after the queue has been started.");

            monitor = new PerformanceMonitor(this);
        }

        public void EnableEndpointPortAutoSelection()
        {
            if (wasStarted)
                throw new InvalidOperationException("Endpoint auto-port-selection cannot be enabled after the queue has been started.");

            enableEndpointPortAutoSelection = true;
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
            if (wasDisposed)
                return;

            DisposeResourcesWhoseDisposalCannotFail();

            if (monitor != null)
                monitor.Dispose();

            queueStorage.Dispose();

            // only after we finish incoming recieves, and finish processing
            // active transactions can we mark it as disposed
            wasDisposed = true;
        }

        public void DisposeRudely()
        {
            if (wasDisposed)
                return;

            DisposeResourcesWhoseDisposalCannotFail();

            queueStorage.DisposeRudely();

            // only after we finish incoming recieves, and finish processing
            // active transactions can we mark it as disposed
            wasDisposed = true;
        }

        private void DisposeResourcesWhoseDisposalCannotFail()
        {
            disposing = true;

            lock (newMessageArrivedLock)
            {
                Monitor.PulseAll(newMessageArrivedLock);
            }

            if (wasStarted)
            {
                purgeOldDataTimer.Dispose();

                queuedMessagesSender.Stop();
                sendingThread.Join();

                receiver.Dispose();
            }

            while (currentlyInCriticalReceiveStatus > 0)
            {
                logger.WarnFormat("Waiting for {0} messages that are currently in critical receive status", currentlyInCriticalReceiveStatus);
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }

            while (currentlyInsideTransaction > 0)
            {
                logger.WarnFormat("Waiting for {0} transactions currently running", currentlyInsideTransaction);
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
        }

        #endregion

        private void OnTransactionComplete()
        {
            lock (newMessageArrivedLock)
            {
                Monitor.PulseAll(newMessageArrivedLock);
            }
            Interlocked.Decrement(ref currentlyInsideTransaction);
        }

        private void AssertNotDisposed()
        {
            if (wasDisposed)
                throw new ObjectDisposedException("QueueManager");
        }


        private void AssertNotDisposedOrDisposing()
        {
            if (disposing || wasDisposed)
                throw new ObjectDisposedException("QueueManager");
        }

        public void WaitForAllMessagesToBeSent()
        {
            waitingForAllMessagesToBeSent = true;
            try
            {
                var hasMessagesToSend = true;
                do
                {
                    queueStorage.Send(actions =>
                    {
                        hasMessagesToSend = actions.HasMessagesToSend();
                        actions.Commit();
                    });
                    if (hasMessagesToSend)
                        Thread.Sleep(100);
                } while (hasMessagesToSend);
            }
            finally
            {
                waitingForAllMessagesToBeSent = false;
            }
        }

        public IQueue GetQueue(string queue)
        {
            return new Queue(this, queue);
        }

        public PersistentMessage[] GetAllMessages(string queueName, string subqueue)
        {
            AssertNotDisposedOrDisposing();
            PersistentMessage[] messages = null;
            queueStorage.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllMessages(subqueue).ToArray();
                actions.Commit();
            });
            return messages;
        }

        public HistoryMessage[] GetAllProcessedMessages(string queueName)
        {
            AssertNotDisposedOrDisposing();
            HistoryMessage[] messages = null;
            queueStorage.Global(actions =>
            {
                messages = actions.GetQueue(queueName).GetAllProcessedMessages().ToArray();
                actions.Commit();
            });
            return messages;
        }

        public PersistentMessageToSend[] GetAllSentMessages()
        {
            AssertNotDisposedOrDisposing();
            PersistentMessageToSend[] msgs = null;
            queueStorage.Global(actions =>
            {
                msgs = actions.GetSentMessages().ToArray();

                actions.Commit();
            });
            return msgs;
        }

        public PersistentMessageToSend[] GetMessagesCurrentlySending()
        {
            AssertNotDisposedOrDisposing();
            PersistentMessageToSend[] msgs = null;
            queueStorage.Send(actions =>
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
                    remaining = Max(TimeSpan.Zero, remaining - sp.Elapsed);
                }
            }
        }

        private static TimeSpan Max(TimeSpan x, TimeSpan y)
        {
            return x >= y ? x : y;
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
            EnsureEnlistment();

            return Receive(Enlistment, queueName, subqueue, timeout);
        }

        public MessageId Send(Uri uri, MessagePayload payload)
        {
            if (waitingForAllMessagesToBeSent)
                throw new CannotSendWhileWaitingForAllMessagesToBeSentException("Currently waiting for all messages to be sent, so we cannot send. You probably have a race condition in your application.");

            EnsureEnlistment();

            return Send(Enlistment, uri, payload);
        }

        private void EnsureEnlistment()
        {
            AssertNotDisposedOrDisposing();

            if (Transaction.Current == null)
                throw new InvalidOperationException("You must use TransactionScope when using Rhino.Queues");

            if (CurrentlyEnslistedTransaction == Transaction.Current)
                return;
            // need to change the enlistment
            Interlocked.Increment(ref currentlyInsideTransaction);
            Enlistment = new TransactionEnlistment(queueStorage, OnTransactionComplete, AssertNotDisposed);
            CurrentlyEnslistedTransaction = Transaction.Current;
        }

        private PersistentMessage GetMessageFromQueue(ITransaction transaction, string queueName, string subqueue)
        {
            AssertNotDisposedOrDisposing();
            PersistentMessage message = null;
            queueStorage.Global(actions =>
            {
                message = actions.GetQueue(queueName).Dequeue(subqueue);

                if (message != null)
                {
                    actions.RegisterUpdateToReverse(
                        transaction.Id,
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
            AssertNotDisposedOrDisposing();
            PersistentMessage message = null;
            queueStorage.Global(actions =>
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

        protected virtual IMessageAcceptance AcceptMessages(Message[] msgs)
        {
            var bookmarks = new List<MessageBookmark>();
            queueStorage.Global(actions =>
            {
                foreach (var msg in receivedMsgs.Filter(msgs, message => message.Id))
                {
                    var queue = actions.GetQueue(msg.Queue);
                    var bookmark = queue.Enqueue(msg);
                    bookmarks.Add(bookmark);
                }
                actions.Commit();
            });
            return new MessageAcceptance(this, bookmarks, msgs, queueStorage);
        }

        #region Nested type: MessageAcceptance

        private class MessageAcceptance : IMessageAcceptance
        {
            private readonly IList<MessageBookmark> bookmarks;
            private readonly IEnumerable<Message> messages;
            private readonly QueueManager parent;
            private readonly QueueStorage queueStorage;

            public MessageAcceptance(QueueManager parent,
                IList<MessageBookmark> bookmarks,
                IEnumerable<Message> messages,
                QueueStorage queueStorage)
            {
                this.parent = parent;
                this.bookmarks = bookmarks;
                this.messages = messages;
                this.queueStorage = queueStorage;
                Interlocked.Increment(ref parent.currentlyInCriticalReceiveStatus);
            }

            #region IMessageAcceptance Members

            public void Commit()
            {
                try
                {
                    parent.AssertNotDisposed();
                    queueStorage.Global(actions =>
                    {
                        foreach (var bookmark in bookmarks)
                        {
                            actions.GetQueue(bookmark.QueueName)
                                .SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                        }
                        foreach (var msg in messages)
                        {
                            actions.MarkReceived(msg.Id);
                        }
                        actions.Commit();
                    });
                    parent.receivedMsgs.Add(messages.Select(m => m.Id));

                    foreach (var msg in messages)
                    {
                        parent.OnMessageQueuedForReceive(msg);
                    }

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
                    parent.AssertNotDisposed();
                    queueStorage.Global(actions =>
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
            AssertNotDisposedOrDisposing();

            queueStorage.Global(actions =>
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
                AssertNotDisposedOrDisposing();
                string[] queues = null;
                queueStorage.Global(actions =>
                {
                    queues = actions.GetAllQueuesNames();

                    actions.Commit();
                });
                return queues;
            }
        }

        public void MoveTo(string subqueue, Message message)
        {
            AssertNotDisposedOrDisposing();
            EnsureEnlistment();

            queueStorage.Global(actions =>
            {
                var queue = actions.GetQueue(message.Queue);
                var bookmark = queue.MoveTo(subqueue, (PersistentMessage)message);
                actions.RegisterUpdateToReverse(Enlistment.Id,
                    bookmark, MessageStatus.ReadyToDeliver,
                    message.SubQueue
                    );
                actions.Commit();
            });

            if (((PersistentMessage)message).Status == MessageStatus.ReadyToDeliver)
                OnMessageReceived(message);

            var updatedMessage = new Message
                                     {
                                         Id = message.Id,
                                         Data = message.Data,
                                         Headers = message.Headers,
                                         Queue = message.Queue,
                                         SubQueue = subqueue,
                                         SentAt = message.SentAt
                                     };

            OnMessageQueuedForReceive(updatedMessage);
        }

        public void EnqueueDirectlyTo(string queue, string subqueue, MessagePayload payload)
        {
            EnsureEnlistment();

            var message = new PersistentMessage
            {
                Data = payload.Data,
                Headers = payload.Headers,
                Id = new MessageId
                {
                    SourceInstanceId = queueStorage.Id,
                    MessageIdentifier = GuidCombGenerator.Generate()
                },
                Queue = queue,
                SentAt = DateTime.Now,
                SubQueue = subqueue,
                Status = MessageStatus.EnqueueWait
            };

            queueStorage.Global(actions =>
            {
                var queueActions = actions.GetQueue(queue);

                var bookmark = queueActions.Enqueue(message);
                actions.RegisterUpdateToReverse(Enlistment.Id, bookmark, MessageStatus.EnqueueWait, subqueue);

                actions.Commit();
            });

            OnMessageQueuedForReceive(message);

            lock (newMessageArrivedLock)
            {
                Monitor.PulseAll(newMessageArrivedLock);
            }
        }

        public PersistentMessage PeekById(string queueName, MessageId id)
        {
            PersistentMessage message = null;
            queueStorage.Global(actions =>
            {
                var queue = actions.GetQueue(queueName);

                message = queue.PeekById(id);

                actions.Commit();
            });
            return message;
        }

        public string[] GetSubqueues(string queueName)
        {
            string[] result = null;
            queueStorage.Global(actions =>
            {
                var queue = actions.GetQueue(queueName);

                result = queue.Subqueues;

                actions.Commit();
            });
            return result;
        }

        public int GetNumberOfMessages(string queueName)
        {
            int numberOfMsgs = 0;
            queueStorage.Global(actions =>
            {
                numberOfMsgs = actions.GetNumberOfMessages(queueName);
                actions.Commit();
            });
            return numberOfMsgs;
        }

        public void FailedToSendTo(Endpoint endpointThatWeFailedToSendTo)
        {
            var action = FailedToSendMessagesTo;
            if (action != null)
                action(endpointThatWeFailedToSendTo);
        }

        public void OnMessageQueuedForSend(MessageEventArgs messageEventArgs)
        {
            var action = MessageQueuedForSend;
            if (action != null) action(this, messageEventArgs);
        }

        public void OnMessageSent(MessageEventArgs messageEventArgs)
        {
            var action = MessageSent;
            if (action != null) action(this, messageEventArgs);
        }

        private void OnMessageQueuedForReceive(Message message)
        {
            OnMessageQueuedForReceive(new MessageEventArgs(null, message));
        }

        public void OnMessageQueuedForReceive(MessageEventArgs messageEventArgs)
        {
            var action = MessageQueuedForReceive;
            if (action != null) action(this, messageEventArgs);
        }

        private void OnMessageReceived(Message message)
        {
            OnMessageReceived(new MessageEventArgs(null, message));
        }

        public void OnMessageReceived(MessageEventArgs messageEventArgs)
        {
            var action = MessageReceived;
            if (action != null) action(this, messageEventArgs);
        }

        public Message Receive(ITransaction transaction, string queueName)
        {
            return Receive(transaction, queueName, null, TimeSpan.FromDays(1));
        }

        public Message Receive(ITransaction transaction, string queueName, TimeSpan timeout)
        {
            return Receive(transaction, queueName, null, timeout);
        }

        public Message Receive(ITransaction transaction, string queueName, string subqueue)
        {
            return Receive(transaction, queueName, subqueue, TimeSpan.FromDays(1));
        }

        public Message Receive(ITransaction transaction, string queueName, string subqueue, TimeSpan timeout)
        {
            var remaining = timeout;
            while (true)
            {
                var message = GetMessageFromQueue(transaction, queueName, subqueue);
                if (message != null)
                {
                    OnMessageReceived(message);
                    return message;
                }
                lock (newMessageArrivedLock)
                {
                    message = GetMessageFromQueue(transaction, queueName, subqueue);
                    if (message != null)
                    {
                        OnMessageReceived(message);
                        return message;
                    }
                    var sp = Stopwatch.StartNew();
                    if (Monitor.Wait(newMessageArrivedLock, remaining) == false)
                        throw new TimeoutException("No message arrived in the specified timeframe " + timeout);
                    var newRemaining = remaining - sp.Elapsed;
                    remaining = newRemaining >= TimeSpan.Zero ? newRemaining : TimeSpan.Zero;
                }
            }
        }

        public MessageId Send(ITransaction transaction, Uri uri, MessagePayload payload)
        {
            if (waitingForAllMessagesToBeSent)
                throw new CannotSendWhileWaitingForAllMessagesToBeSentException("Currently waiting for all messages to be sent, so we cannot send. You probably have a race condition in your application.");

            var parts = uri.AbsolutePath.Substring(1).Split('/');
            var queue = parts[0];
            string subqueue = null;
            if (parts.Length > 1)
            {
                subqueue = string.Join("/", parts.Skip(1).ToArray());
            }

            Guid msgId = Guid.Empty;

            var port = uri.Port;
            if (port == -1)
                port = 2200;
            var destination = new Endpoint(uri.Host, port);

            queueStorage.Global(actions =>
            {
                msgId = actions.RegisterToSend(destination, queue,
                                               subqueue, payload, transaction.Id);

                actions.Commit();
            });

            var messageId = new MessageId
                                {
                                    SourceInstanceId = queueStorage.Id,
                                    MessageIdentifier = msgId
                                };
            var message = new Message
            {
                Id = messageId,
                Data = payload.Data,
                Headers = payload.Headers,
                Queue = queue,
                SubQueue = subqueue
            };

            OnMessageQueuedForSend(new MessageEventArgs(destination, message));

            return messageId;
        }
    }
}
#pragma warning restore 420
