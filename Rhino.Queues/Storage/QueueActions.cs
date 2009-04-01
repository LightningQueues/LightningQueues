using System;
using System.Collections.Generic;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;

namespace Rhino.Queues.Storage
{
    public class QueueActions : IDisposable
    {
        private readonly ILog logger = LogManager.GetLogger(typeof (QueueActions));
        private readonly Session session;
        private readonly string queueName;
        private readonly Action<int> changeNumberOfMessages;
        private readonly Table msgs;
        private readonly Table msgsHistory;
        private readonly Dictionary<string, JET_COLUMNID> msgsColumns;
        private readonly Dictionary<string, JET_COLUMNID> msgsHistoryColumns;

        public QueueActions(Session session, JET_DBID dbid, string queueName, Action<int> changeNumberOfMessages)
        {
            this.session = session;
            this.queueName = queueName;
            this.changeNumberOfMessages = changeNumberOfMessages;
            msgs = new Table(session, dbid, queueName, OpenTableGrbit.None);
            msgsColumns = Api.GetColumnDictionary(session, msgs);
            msgsHistory = new Table(session, dbid, queueName+"_history", OpenTableGrbit.None);
            msgsHistoryColumns = Api.GetColumnDictionary(session, msgsHistory);
        }

        public MessageBookmark Enqueue(Message message)
        {
            var bm = new MessageBookmark {QueueName = queueName};
            using (var updateMsgs = new Update(session, msgs, JET_prep.Insert))
            {
                Api.SetColumn(session, msgs, msgsColumns["timestamp"], message.SentAt.ToOADate());
                Api.SetColumn(session, msgs, msgsColumns["data"], message.Data);
                Api.SetColumn(session, msgs, msgsColumns["instance_id"], message.Id.Guid.ToByteArray());
                Api.SetColumn(session, msgs, msgsColumns["msg_number"], message.Id.Number);
                Api.SetColumn(session, msgs, msgsColumns["subqueue"], message.SubQueue, Encoding.Unicode);
                Api.SetColumn(session, msgs, msgsColumns["status"], (int)MessageStatus.InTransit);

                updateMsgs.Save(bm.Bookmark, bm.Size, out bm.Size);
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

            if (Api.TrySeek(session, msgs, SeekGrbit.SeekEQ) == false)
                return null;

            Api.MakeKey(session, msgs, subqueue, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(session, msgs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            do
            {
                var id = new MessageId
                {
                    Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                    Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
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
                        Api.SetColumn(session, msgs, msgsColumns["status"], (int) MessageStatus.Processing);
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
                var bookmark = new MessageBookmark {QueueName = queueName};
                Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
                changeNumberOfMessages(-1);
                
                logger.DebugFormat("Dequeuing message {2} from '{0}/{1}'",
                                   queueName, subqueue, id);
                
                return new PersistentMessage
                {
                    Bookmark = bookmark,
                    Queue = queueName,
                    SentAt =
                        DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
                    Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
                    Id = id
                };
            } while (Api.TryMoveNext(session, msgs));

            return null;
        }

        public void SetMessageStatus(MessageBookmark bookmark, MessageStatus status, string subqueue)
        {
            Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
            var id = new MessageId
            {
                Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
            };
            using (var update = new Update(session, msgs, JET_prep.Replace))
            {
                Api.SetColumn(session, msgs, msgsColumns["status"], (int)status);
                Api.SetColumn(session, msgs, msgsColumns["subqueue"], subqueue,Encoding.Unicode);
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
                Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
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
                Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
            }; 
            using(var update = new Update(session, msgsHistory, JET_prep.Insert))
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

        public IEnumerable<PersistentMessage> GetAllMessages()
        {
             Api.MoveBeforeFirst(session, msgs);
             while (Api.TryMoveNext(session, msgs))
             {
                 var bookmark = new MessageBookmark { QueueName = queueName };
                 Api.JetGetBookmark(session, msgs, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
                 yield return new PersistentMessage 
                 {
                     Bookmark = bookmark,
                     Queue = queueName,
                     LocalId = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["local_id"]).Value,
                     Status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value,
                     SentAt =
                         DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgs, msgsColumns["timestamp"]).Value),
                     Data = Api.RetrieveColumn(session, msgs, msgsColumns["data"]),
                     Id = new MessageId
                     {
                         Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                         Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
                     }
                 };
             }
        }

        public IEnumerable<HistoryMessage> GetAllProcessedMessages()
        {
            Api.MoveBeforeFirst(session, msgsHistory);
            while (Api.TryMoveNext(session, msgsHistory))
            {
                var bookmark = new MessageBookmark { QueueName = queueName };
                Api.JetGetBookmark(session, msgsHistory, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
                yield return new HistoryMessage
                {
                    Bookmark = bookmark,
                    Queue = queueName,
                    MovedToHistoryAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgsHistory, msgsHistoryColumns["moved_to_history_at"]).Value),
                    LocalId = Api.RetrieveColumnAsInt32(session, msgsHistory, msgsHistoryColumns["local_id"]).Value,
                    Status = (MessageStatus)Api.RetrieveColumnAsInt32(session, msgsHistory, msgsHistoryColumns["status"]).Value,
                    SentAt =
                        DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, msgsHistory, msgsHistoryColumns["timestamp"]).Value),
                    Data = Api.RetrieveColumn(session, msgsHistory, msgsHistoryColumns["data"]),
                    Id = new MessageId
                    {
                        Number = Api.RetrieveColumnAsInt32(session, msgsHistory, msgsHistoryColumns["msg_number"]).Value,
                        Guid = new Guid(Api.RetrieveColumn(session, msgsHistory, msgsHistoryColumns["instance_id"]))
                    }
                };
            }
        }

        public MessageBookmark MoveTo(string subQueue, PersistentMessage message)
        {
            Api.JetGotoBookmark(session, msgs, message.Bookmark.Bookmark, message.Bookmark.Size);
            var id = new MessageId
            {
                Number = Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["msg_number"]).Value,
                Guid = new Guid(Api.RetrieveColumn(session, msgs, msgsColumns["instance_id"]))
            }; 
            using(var update = new Update(session, msgs, JET_prep.Replace))
            {
                Api.SetColumn(session, msgs, msgsColumns["status"], (int) MessageStatus.SubqueueChanged);
                Api.SetColumn(session, msgs, msgsColumns["subqueue"], subQueue,Encoding.Unicode);

                var bookmark = new MessageBookmark {QueueName = queueName};
                update.Save(bookmark.Bookmark,bookmark.Size, out bookmark.Size);

                logger.DebugFormat("Moving message {0} to subqueue {1}",
                               id, queueName);
                return bookmark;
            }
        }

        public MessageStatus GetMessageStatus(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
            return (MessageStatus) Api.RetrieveColumnAsInt32(session, msgs, msgsColumns["status"]).Value;
        }

        public void Discard(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, msgs, bookmark.Bookmark, bookmark.Size);
            Api.JetDelete(session, msgs);
        }
    }
}