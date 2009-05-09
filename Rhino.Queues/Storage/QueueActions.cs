using System;
using System.Collections.Generic;
using System.Text;
using System.Web;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
	using System.Linq;

	public class QueueActions : IDisposable
	{
		private readonly ILog logger = LogManager.GetLogger(typeof(QueueActions));
		private readonly Session session;
		private readonly string queueName;
		private string[] subqueues;
		private readonly AbstractActions actions;
		private readonly Action<int> changeNumberOfMessages;
		private readonly Table msgs;
		private readonly Table msgsHistory;
		private readonly Dictionary<string, JET_COLUMNID> msgsColumns;
		private readonly Dictionary<string, JET_COLUMNID> msgsHistoryColumns;

		public QueueActions(Session session, JET_DBID dbid, string queueName, string[] subqueues, AbstractActions actions, Action<int> changeNumberOfMessages)
		{
			this.session = session;
			this.queueName = queueName;
			this.subqueues = subqueues;
			this.actions = actions;
			this.changeNumberOfMessages = changeNumberOfMessages;
			msgs = new Table(session, dbid, queueName, OpenTableGrbit.None);
			msgsColumns = Api.GetColumnDictionary(session, msgs);
			msgsHistory = new Table(session, dbid, queueName + "_history", OpenTableGrbit.None);
			msgsHistoryColumns = Api.GetColumnDictionary(session, msgsHistory);
		}

		public string[] Subqueues
		{
			get
			{
				return subqueues;
			}
		}

		public MessageBookmark Enqueue(Message message)
		{
			var bm = new MessageBookmark { QueueName = queueName };
			using (var updateMsgs = new Update(session, msgs, JET_prep.Insert))
			{
				var messageStatus = MessageStatus.InTransit;
				var persistentMessage = message as PersistentMessage;
				if (persistentMessage != null)
					messageStatus = persistentMessage.Status;

				Api.SetColumn(session, msgs, msgsColumns["timestamp"], message.SentAt.ToOADate());
				Api.SetColumn(session, msgs, msgsColumns["data"], message.Data);
				Api.SetColumn(session, msgs, msgsColumns["instance_id"], message.Id.SourceInstanceId.ToByteArray());
				Api.SetColumn(session, msgs, msgsColumns["msg_id"], message.Id.MessageIdentifier.ToByteArray());
				Api.SetColumn(session, msgs, msgsColumns["subqueue"], message.SubQueue, Encoding.Unicode);
				Api.SetColumn(session, msgs, msgsColumns["headers"], message.Headers.ToQueryString(), Encoding.Unicode);
				Api.SetColumn(session, msgs, msgsColumns["status"], (int)messageStatus);

				updateMsgs.Save(bm.Bookmark, bm.Size, out bm.Size);
			}
			if (string.IsNullOrEmpty(message.SubQueue) == false &&
				Subqueues.Contains(message.SubQueue) == false)
			{
				actions.AddSubqueueTo(queueName, message.SubQueue);
				subqueues = subqueues.Union(new[] { message.SubQueue }).ToArray();
			}

			logger.DebugFormat("Enqueuing msg to '{0}' with subqueue: '{1}'. Id: {2}", queueName,
				message.SubQueue,
				message.Id);
			changeNumberOfMessages(1);
			return bm;
		}

		public PersistentMessage Dequeue(string subqueue)
		{
			Api.JetSetCurrentIndex(session, msgs, "by_sub_queue");
			Api.MakeKey(session, msgs, subqueue, Encoding.Unicode, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, msgs, SeekGrbit.SeekGE) == false)
				return null;

			Api.MakeKey(session, msgs, subqueue, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
			try
			{
				Api.JetSetIndexRange(session, msgs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
				return null;
			}

			do
			{
				var id = new MessageId
				{
					MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
					SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
				};

				var status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value;

				logger.DebugFormat("Scanning incoming message {2} on '{0}/{1}' with status {3}",
					queueName, subqueue, id, status);

				if (status != MessageStatus.ReadyToDeliver)
					continue;

				try
				{
					using (var update = new Update(session, msgs, JET_prep.Replace))
					{
						Api.SetColumn(session, msgs, msgsColumns["status"], (int)MessageStatus.Processing);
						update.Save();
					}
				}
				catch (EsentErrorException e)
				{
					logger.DebugFormat("Write conflict on '{0}/{1}' for {2}, skipping message",
									   queueName, subqueue, id);
					if (e.Error == JET_err.WriteConflict)
						continue;
					throw;
				}
				var bookmark = new MessageBookmark { QueueName = queueName };
				Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
				changeNumberOfMessages(-1);

				logger.DebugFormat("Dequeuing message {2} from '{0}/{1}'",
								   queueName, subqueue, id);

				var headersAsQueryString = Api.RetrieveColumnAsString(session, msgs, msgsColumns["headers"]);

				return new PersistentMessage
				{
					Bookmark = bookmark,
					Headers = HttpUtility.ParseQueryString(headersAsQueryString),
					Queue = queueName,
					SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
					Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
					Id = id,
					SubQueue = subqueue,
					Status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value
				};
			} while (Api.TryMoveNext(session, msgs));

			return null;
		}

		public void SetMessageStatus(MessageBookmark bookmark, MessageStatus status, string subqueue)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
			var id = new MessageId
			{
				MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
				SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
			};
			using (var update = new Update(session, msgs, JET_prep.Replace))
			{
				Api.SetColumn(session, msgs, msgsColumns["status"], (int)status);
				Api.SetColumn(session, msgs, msgsColumns["subqueue"], subqueue, Encoding.Unicode);
				update.Save();
			}
			logger.DebugFormat("Changing message {0} status to {1} on queue '{2}' and set subqueue to '{3}'",
							   id, status, queueName, subqueue);
		}

		public void SetMessageStatus(MessageBookmark bookmark, MessageStatus status)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);

			var id = new MessageId
			{
				MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
				SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
			};

			using (var update = new Update(session, msgs, JET_prep.Replace))
			{
				Api.SetColumn(session, msgs, msgsColumns["status"], (int)status);
				update.Save();
			}

			logger.DebugFormat("Changing message {0} status to {1} on {2}",
							   id, status, queueName);
		}

		public void Dispose()
		{
			if (msgs != null)
				msgs.Dispose();
			if (msgsHistory != null)
				msgsHistory.Dispose();
		}

		public void MoveToHistory(MessageBookmark bookmark)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
			var id = new MessageId
			{
				MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
				SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
			};
			using (var update = new Update(session, msgsHistory, JET_prep.Insert))
			{
				foreach (var column in msgsColumns.Keys)
				{
					Api.SetColumn(session, msgsHistory, msgsHistoryColumns[column],
								  Api.RetrieveColumn(session, msgs, msgsColumns[column]));
				}
				Api.SetColumn(session, msgsHistory, msgsHistoryColumns["moved_to_history_at"],
					DateTime.Now.ToOADate());
				update.Save();
			}
			logger.DebugFormat("Moving message {0} on queue {1}",
							   id, queueName);
		}

		public IEnumerable<PersistentMessage> GetAllMessages(string subQueue)
		{
			Api.JetSetCurrentIndex(session, msgs, "by_sub_queue");
			Api.MakeKey(session, msgs, subQueue, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, msgs, SeekGrbit.SeekGE) == false)
				yield break;
			Api.MakeKey(session, msgs, subQueue, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
			try
			{
				Api.JetSetIndexRange(session, msgs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
				yield break;
			}
			do
			{
				var bookmark = new MessageBookmark { QueueName = queueName };
				Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
				var headersAsQueryString = Api.RetrieveColumnAsString(session, msgs, msgsColumns["headers"]);
				yield return new PersistentMessage
				{
					Bookmark = bookmark,
					Headers = HttpUtility.ParseQueryString(headersAsQueryString),
					Queue = queueName,
					Status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value,
					SentAt =
						DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
					Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
					Id = new MessageId
					{
						MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
						SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
					}
				};
			} while (Api.TryMoveNext(session, msgs));
		}

		public IEnumerable<HistoryMessage> GetAllProcessedMessages()
		{
			Api.MoveBeforeFirst(session, msgsHistory);
			while (Api.TryMoveNext(session, msgsHistory))
			{
				var bookmark = new MessageBookmark { QueueName = queueName };
				Api.JetGetBookmark(session, msgsHistory, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
				var headersAsQueryString = Api.RetrieveColumnAsString(session, msgsHistory,
																	  msgsHistoryColumns["headers"]);
				yield return new HistoryMessage
				{
					Bookmark = bookmark,
					Headers = HttpUtility.ParseQueryString(headersAsQueryString),
					Queue = queueName,
					MovedToHistoryAt =
						DateTime.FromOADate(
						Api.RetrieveColumnAsDouble(session, msgsHistory, msgsHistoryColumns["moved_to_history_at"]).
							Value),
					Status =
						(MessageStatus)
						Api.RetrieveColumnAsInt32(session, msgsHistory, msgsHistoryColumns["status"]).Value,
					SentAt =
						DateTime.FromOADate(
						Api.RetrieveColumnAsDouble(session, msgsHistory, msgsHistoryColumns["timestamp"]).Value),
					Data = Api.RetrieveColumn(session, msgsHistory, msgsHistoryColumns["data"]),
					Id = new MessageId
					{
						MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgsHistory, msgsHistoryColumns["msg_id"])),
						SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgsHistory, msgsHistoryColumns["instance_id"]))
					}
				};
			}
		}

		public MessageBookmark MoveTo(string subQueue, PersistentMessage message)
		{
			Api.JetGotoBookmark(session, msgs, message.Bookmark.Bookmark, message.Bookmark.Size);
			var id = new MessageId
			{
				MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
				SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
			};
			using (var update = new Update(session, msgs, JET_prep.Replace))
			{
				Api.SetColumn(session, msgs, msgsColumns["status"], (int)MessageStatus.SubqueueChanged);
				Api.SetColumn(session, msgs, msgsColumns["subqueue"], subQueue, Encoding.Unicode);

				var bookmark = new MessageBookmark { QueueName = queueName };
				update.Save(bookmark.Bookmark, bookmark.Size, out bookmark.Size);

				logger.DebugFormat("Moving message {0} to subqueue {1}",
							   id, queueName);
				return bookmark;
			}
		}

		public MessageStatus GetMessageStatus(MessageBookmark bookmark)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
			return (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value;
		}

		public void Discard(MessageBookmark bookmark)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
			Api.JetDelete(session, msgs);
		}

		public PersistentMessage Peek(string subqueue)
		{
			Api.JetSetCurrentIndex(session, msgs, "by_sub_queue");
			Api.MakeKey(session, msgs, subqueue, Encoding.Unicode, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, msgs, SeekGrbit.SeekGE) == false)
				return null;

			Api.MakeKey(session, msgs, subqueue, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
			try
			{
				Api.JetSetIndexRange(session, msgs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
				return null;
			}

			do
			{
				var id = new MessageId
				{
					MessageIdentifier = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["msg_id"])),
					SourceInstanceId = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
				};

				var status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value;

				logger.DebugFormat("Scanning incoming message {2} on '{0}/{1}' with status {3}",
								   queueName, subqueue, id, status);

				if (status != MessageStatus.ReadyToDeliver)
					continue;


				var bookmark = new MessageBookmark { QueueName = queueName };
				Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
				changeNumberOfMessages(-1);

				logger.DebugFormat("Peeking message {2} from '{0}/{1}'",
								   queueName, subqueue, id);

				var headersAsQueryString = Api.RetrieveColumnAsString(session, msgs, msgsColumns["headers"]);

				return new PersistentMessage
				{
					Bookmark = bookmark,
					Headers = HttpUtility.ParseQueryString(headersAsQueryString),
					Queue = queueName,
					SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
					Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
					Id = id,
					SubQueue = subqueue,
					Status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value
				};
			} while (Api.TryMoveNext(session, msgs));

			return null;
		}

		public PersistentMessage PeekById(MessageId id)
		{
			Api.JetSetCurrentIndex(session, msgs, "by_id");
			Api.MakeKey(session, msgs, id.SourceInstanceId.ToByteArray(), MakeKeyGrbit.NewKey);
			Api.MakeKey(session, msgs, id.MessageIdentifier, MakeKeyGrbit.None);

			if (Api.TrySeek(session, msgs, SeekGrbit.SeekEQ) == false)
				return null;

			Api.MakeKey(session, msgs, id.SourceInstanceId.ToByteArray(), MakeKeyGrbit.NewKey);
			Api.MakeKey(session, msgs, id.MessageIdentifier, MakeKeyGrbit.None);
			try
			{
				Api.JetSetIndexRange(session, msgs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
				return null;
			}

			do
			{
				var bookmark = new MessageBookmark();
				Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

				var headersAsQueryString = Api.RetrieveColumnAsString(session, msgs, msgsColumns["headers"]);
				var subqueue = Api.RetrieveColumnAsString(session, msgs, msgsColumns["subqueue"]);

				var status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value;

				if (status != MessageStatus.ReadyToDeliver)
					continue;

				return new PersistentMessage
				{
					Bookmark = bookmark,
					Headers = HttpUtility.ParseQueryString(headersAsQueryString),
					Queue = queueName,
					SentAt =
						DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
					Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
					Id = id,
					SubQueue = subqueue,
					Status = status
				};
			} while (Api.TryMoveNext(session, msgs));

			return null;
		}

		public void Delete(MessageBookmark bookmark)
		{
			Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
			Api.JetDelete(session, msgs);
		}

		public void DeleteHistoric(MessageBookmark bookmark)
		{
			Api.JetGotoBookmark(session, msgsHistory, bookmark.Bookmark, bookmark.Size);
			Api.JetDelete(session, msgsHistory);
		}
	}
}
