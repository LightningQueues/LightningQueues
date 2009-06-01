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
	using Exceptions;
	using Utils;

	public class QueueManager : IQueueManager
	{
		[ThreadStatic]
		private static TransactionEnlistment Enlistment;

		[ThreadStatic]
		private static Transaction CurrentlyEnslistedTransaction;

		private volatile bool wasDisposed;
		private volatile int currentlyInCriticalReceiveStatus;
		private volatile int currentlyInsideTransaction;
		private readonly IPEndPoint endpoint;
		private readonly object newMessageArrivedLock = new object();
		private readonly string path;
		private readonly Timer purgeOldDataTimer;
		private readonly QueueStorage queueStorage;
		private readonly Receiver receiver;
		private readonly Thread sendingThread;
		private readonly QueuedMessagesSender queuedMessagesSender;
		private readonly ILog logger = LogManager.GetLogger(typeof(QueueManager));
		private volatile bool waitingForAllMessagesToBeSent;

		private readonly ThreadSafeSet<MessageId> receivedMsgs = new ThreadSafeSet<MessageId>();
		private bool disposing;

		public int NumberOfReceivedMessagesToKeep { get; set; }
		public int? NumberOfMessagesToKeepInProcessedQueues { get; set; }
		public int? NumberOfMessagesToKeepOutgoingQueues { get; set; }

		public TimeSpan? OldestMessageInProcessedQueues { get; set; }
		public TimeSpan? OldestMessageInOutgoingQueues { get; set; }

		public event Action<Endpoint> FailedToSendMessagesTo;

		public QueueManager(IPEndPoint endpoint, string path)
		{
			NumberOfMessagesToKeepInProcessedQueues = 100;
			NumberOfMessagesToKeepOutgoingQueues = 100;
			NumberOfReceivedMessagesToKeep = 100000;
			OldestMessageInProcessedQueues = TimeSpan.FromDays(3);
			OldestMessageInOutgoingQueues = TimeSpan.FromDays(3);

			this.endpoint = endpoint;
			this.path = path;
			queueStorage = new QueueStorage(path);
			queueStorage.Initialize();

			queueStorage.Global(actions =>
			{
				receivedMsgs.Add(actions.GetAlreadyReceivedMessageIds());

				actions.Commit();
			});

			receiver = new Receiver(endpoint, AcceptMessages);
			receiver.Start();

			HandleRecovery();

			queuedMessagesSender = new QueuedMessagesSender(queueStorage, this);
			sendingThread = new Thread(queuedMessagesSender.Send)
			{
				IsBackground = true,
				Name = "Rhino Queue Sender Thread for " + path
			};
			sendingThread.Start();
			purgeOldDataTimer = new Timer(PurgeOldData, null,
				TimeSpan.FromMinutes(3),
				TimeSpan.FromMinutes(3));
		}

		private void PurgeOldData(object ignored)
		{
			logger.DebugFormat("Starting to purge old data");
			try
			{
				queueStorage.Global(actions =>
				{
					foreach (var queue in Queues)
					{
						var queueActions = actions.GetQueue(queue);
						var messages = queueActions.GetAllProcessedMessages();
						if (NumberOfMessagesToKeepInProcessedQueues != null)
							messages = messages.Skip(NumberOfMessagesToKeepInProcessedQueues.Value);
						if (OldestMessageInProcessedQueues != null)
							messages = messages.Where(x => (DateTime.Now - x.SentAt) > OldestMessageInProcessedQueues.Value);

						foreach (var message in messages)
						{
							logger.DebugFormat("Purging message {0} from queue {0}/{1}", message.Id, message.Queue, message.SubQueue);
							queueActions.DeleteHistoric(message.Bookmark);
						}
					}
					var sentMessages = actions.GetSentMessages();

					if (NumberOfMessagesToKeepOutgoingQueues != null)
						sentMessages = sentMessages.Skip(NumberOfMessagesToKeepOutgoingQueues.Value);
					if (OldestMessageInOutgoingQueues != null)
						sentMessages = sentMessages.Where(x => (DateTime.Now - x.SentAt) > OldestMessageInOutgoingQueues.Value);

					foreach (var sentMessage in sentMessages)
					{
						logger.DebugFormat("Purging sent message {0} to {1}/{2}/{3}", sentMessage.Id, sentMessage.Endpoint,
										   sentMessage.Queue, sentMessage.SubQueue);
						actions.DeleteMessageToSendHistoric(sentMessage.Bookmark);
					}

					receivedMsgs.Remove(actions.DeleteOldestReceivedMessages(NumberOfReceivedMessagesToKeep));

					actions.Commit();
				});
			}
			catch (Exception exception)
			{
				logger.Warn("Failed to purge old data from the system", exception);
			}
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

			disposing = true;

			lock (newMessageArrivedLock)
			{
				Monitor.PulseAll(newMessageArrivedLock);
			}

			purgeOldDataTimer.Dispose();

			queuedMessagesSender.Stop();
			sendingThread.Join();

			receiver.Dispose();

			while (currentlyInCriticalReceiveStatus > 0)
			{
				logger.WarnFormat("Waiting for {0} messages that are currently in critical receive status", currentlyInCriticalReceiveStatus);
				Thread.Sleep(TimeSpan.FromSeconds(1));
			}

			while (currentlyInsideTransaction > 0 )
			{
				logger.WarnFormat("Waiting for {0} transactions currently running", currentlyInsideTransaction);
				Thread.Sleep(TimeSpan.FromSeconds(1));
			}

			// only after we finish incoming recieves, and finish processing
			// active transactions can we mark it as disposed
			wasDisposed = true; 
			queueStorage.Dispose();
		}

		#endregion

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
			if (waitingForAllMessagesToBeSent)
				throw new CannotSendWhileWaitingForAllMessagesToBeSentException("Currently waiting for all messages to be sent, so we cannot send. You probably have a race condition in your application.");

			EnsureEnslistment();

			var parts = uri.AbsolutePath.Substring(1).Split('/');
			var queue = parts[0];
			string subqueue = null;
			if (parts.Length > 1)
			{
				subqueue = string.Join("/", parts.Skip(1).ToArray());
			}

			Guid msgId = Guid.Empty;
			queueStorage.Global(actions =>
			{
				var port = uri.Port;
				if (port == -1)
					port = 2200;
				msgId = actions.RegisterToSend(new Endpoint(uri.Host, port), queue,
											   subqueue, payload, Enlistment.Id);

				actions.Commit();
			});
			return new MessageId
			{
				SourceInstanceId = queueStorage.Id,
				MessageIdentifier = msgId
			};
		}

		private void EnsureEnslistment()
		{
			AssertNotDisposedOrDisposing();

			if (Transaction.Current == null)
				throw new InvalidOperationException("You must use TransactionScope when using Rhino.Queues");

			if (CurrentlyEnslistedTransaction == Transaction.Current)
				return;
			// need to change the enslitment
#pragma warning disable 420
			Interlocked.Increment(ref currentlyInsideTransaction);
#pragma warning restore 420
			Enlistment = new TransactionEnlistment(queueStorage, () =>
			{
				lock (newMessageArrivedLock)
				{
					Monitor.PulseAll(newMessageArrivedLock);
				}
#pragma warning disable 420
				Interlocked.Decrement(ref currentlyInsideTransaction);
#pragma warning restore 420
			}, AssertNotDisposed);
			CurrentlyEnslistedTransaction = Transaction.Current;
		}

		private PersistentMessage GetMessageFromQueue(string queueName, string subqueue)
		{
			AssertNotDisposedOrDisposing();
			PersistentMessage message = null;
			queueStorage.Global(actions =>
			{
				message = actions.GetQueue(queueName).Dequeue(subqueue);

				if (message != null)
				{
					actions.RegisterUpdateToReverse(
						Enlistment.Id,
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

		private IMessageAcceptance AcceptMessages(Message[] msgs)
		{
			var bookmarks = new List<MessageBookmark>();
			queueStorage.Global(actions =>
			{
				foreach (var msg in receivedMsgs.Filter(msgs,message => message.Id))
				{
					var queue = actions.GetQueue(msg.Queue);
					var bookmark = queue.Enqueue(msg);
					bookmarks.Add(bookmark);
				}
				actions.Commit();
			});
			var msgIds = msgs.Select(m=>m.Id).ToArray();
			return new MessageAcceptance(this, bookmarks, msgIds, queueStorage);
		}

		#region Nested type: MessageAcceptance

		private class MessageAcceptance : IMessageAcceptance
		{
			private readonly IList<MessageBookmark> bookmarks;
			private readonly IEnumerable<MessageId> messageIds;
			private readonly QueueManager parent;
			private readonly QueueStorage queueStorage;

			public MessageAcceptance(QueueManager parent,
				IList<MessageBookmark> bookmarks, 
				IEnumerable<MessageId> messageIds,
				QueueStorage queueStorage)
			{
				this.parent = parent;
				this.bookmarks = bookmarks;
				this.messageIds = messageIds;
				this.queueStorage = queueStorage;
#pragma warning disable 420
				Interlocked.Increment(ref parent.currentlyInCriticalReceiveStatus);
#pragma warning restore 420
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
						foreach (var id in messageIds)
						{
							actions.MarkReceived(id);
						}
						actions.Commit();
					});
					parent.receivedMsgs.Add(messageIds);

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
			EnsureEnslistment();

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
		}

		public void EnqueueDirectlyTo(string queue, string subqueue, MessagePayload payload)
		{
			EnsureEnslistment();

			queueStorage.Global(actions =>
			{
				var queueActions = actions.GetQueue(queue);

				var bookmark = queueActions.Enqueue(new PersistentMessage
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
				});
				actions.RegisterUpdateToReverse(Enlistment.Id, bookmark, MessageStatus.EnqueueWait, subqueue);

				actions.Commit();
			});
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
	}
}
