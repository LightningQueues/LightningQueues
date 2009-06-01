using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
    public class SenderActions : AbstractActions
    {
        private readonly Guid instanceId;
        private readonly ILog logger = LogManager.GetLogger(typeof (SenderActions));

        public SenderActions(JET_INSTANCE instance, string database, Guid instanceId)
            : base(instance, database)
        {
            this.instanceId = instanceId;
        }

        public IList<PersistentMessage> GetMessagesToSendAndMarkThemAsInFlight(int maxNumberOfMessage, int maxSizeOfMessagesInTotal, out Endpoint endPoint)
        {
            Api.MoveBeforeFirst(session, outgoing);

            endPoint = null;
        	string queue = null;
            var messages = new List<PersistentMessage>();

            while (Api.TryMoveNext(session, outgoing))
            {

				var msgId = new Guid(Api.RetrieveColumn(session, outgoing, outgoingColumns["msg_id"]));
                var value = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["send_status"]).Value;
                var timeAsDate = Api.RetrieveColumnAsDouble(session, outgoing, outgoingColumns["time_to_send"]).Value;
                var time = DateTime.FromOADate(timeAsDate);

                logger.DebugFormat("Scanning message {0} with status {1} to be sent at {2}", msgId, value, time);
                if (value != OutgoingMessageStatus.Ready)
                    continue;

                if (time > DateTime.Now)
                    continue;

                var rowEndpoint = new Endpoint(
                    Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["address"]),
                    Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["port"]).Value
                    );

                if (endPoint == null)
                    endPoint = rowEndpoint;

                if (endPoint.Equals(rowEndpoint) == false)
                    continue;

				var rowQueue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["queue"], Encoding.Unicode);

				if (queue == null) 
					queue = rowQueue;

				if(queue != rowQueue)
					continue;

				logger.DebugFormat("Adding message {0} to returned messages", msgId);
               
                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
                var headerAsQueryString = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["headers"],Encoding.Unicode);
            	messages.Add(new PersistentMessage
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
                        MessageIdentifier = msgId
                    },
                    Headers = HttpUtility.ParseQueryString(headerAsQueryString),
                    Queue = rowQueue,
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, outgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, outgoingColumns["data"]),
                    Bookmark = bookmark
                });

                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    Api.SetColumn(session, outgoing, outgoingColumns["send_status"],
                                  (int)OutgoingMessageStatus.InFlight);
                    update.Save();
                }

                logger.DebugFormat("Marking output message {0} as InFlight", msgId);

                if (maxNumberOfMessage < messages.Count)
                    break;
                if (maxSizeOfMessagesInTotal < messages.Sum(x => x.Data.Length))
                    break;
            }
            return messages;
        }

        public void MarkOutgoingMessageAsFailedTransmission(MessageBookmark bookmark, bool queueDoesNotExistsInDestination)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            var numOfRetries = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["number_of_retries"]).Value;
            var msgId = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value;

            if (numOfRetries < 100 && queueDoesNotExistsInDestination == false)
            {
                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    var timeToSend = DateTime.Now.AddSeconds(numOfRetries * numOfRetries);


                    Api.SetColumn(session, outgoing, outgoingColumns["send_status"], (int)OutgoingMessageStatus.Ready);
                    Api.SetColumn(session, outgoing, outgoingColumns["time_to_send"],
                                  timeToSend.ToOADate());
                    Api.SetColumn(session, outgoing, outgoingColumns["number_of_retries"],
                                  numOfRetries + 1);

                    logger.DebugFormat("Marking outgoing message {0} as failed with retries: {1}",
                                       msgId, numOfRetries + 1);

                    update.Save();
                }
            }
            else
            {
                using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
                {
                    foreach (var column in outgoingColumns.Keys)
                    {
                        Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns[column],
                            Api.RetrieveColumn(session, outgoing, outgoingColumns[column])
                            );
                    }
                    Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns["send_status"],
                        (int)OutgoingMessageStatus.Failed);

                    logger.DebugFormat("Marking outgoing message {0} as permenantly failed after {1} retries",
                                       msgId, numOfRetries + 1);

                    update.Save();
                }
                Api.JetDelete(session, outgoing);
            }
        }

        public MessageBookmark MarkOutgoingMessageAsSuccessfullySent(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            var newBookmark = new MessageBookmark();
            using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
            {
                foreach (var column in outgoingColumns.Keys)
                {
                	var bytes = Api.RetrieveColumn(session, outgoing, outgoingColumns[column]);
                	Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns[column],bytes);
                }
            	Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns["send_status"],
                              (int)OutgoingMessageStatus.Sent);

                update.Save(newBookmark.Bookmark, newBookmark.Size, out newBookmark.Size);
            }
            var msgId = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value;
            Api.JetDelete(session, outgoing);
            logger.DebugFormat("Successfully sent output message {0}", msgId);
            return newBookmark;
        }

    	public bool HasMessagesToSend()
		{
			Api.MoveBeforeFirst(session, outgoing);
			return Api.TryMoveNext(session, outgoing);
		}

        public IEnumerable<PersistentMessageToSend> GetMessagesToSend()
        {
            Api.MoveBeforeFirst(session, outgoing);

            while (Api.TryMoveNext(session, outgoing))
            {
                var address = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["address"]);
                var port = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["port"]).Value;

                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                yield return new PersistentMessageToSend
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
						MessageIdentifier = new Guid(Api.RetrieveColumn(session, outgoing, outgoingColumns["msg_id"]))
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["queue"], Encoding.Unicode),
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, outgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, outgoingColumns["data"]),
                    Bookmark = bookmark
                };
            }
        }

        public void RevertBackToSend(MessageBookmark[] bookmarks)
        {
            foreach (var bookmark in bookmarks)
            {
                Api.JetGotoBookmark(session, outgoingHistory, bookmark.Bookmark, bookmark.Size);
                var msgId = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value;

                using(var update = new  Update(session, outgoing, JET_prep.Insert))
                {
                    foreach (var column in outgoingColumns.Keys)
                    {
                        Api.SetColumn(session, outgoing, outgoingColumns[column],
                            Api.RetrieveColumn(session, outgoingHistory, outgoingHistoryColumns[column])
                            );
                    }
					Api.SetColumn(session, outgoing, outgoingColumns["send_status"],
						(int)OutgoingMessageStatus.Ready);
					Api.SetColumn(session, outgoing, outgoingColumns["number_of_retries"],
						Api.RetrieveColumnAsInt32(session, outgoingHistory, outgoingHistoryColumns["number_of_retries"]).Value + 1
						   );

                    logger.DebugFormat("Reverting output message {0} back to Ready mode", msgId);

                    update.Save();
                }
            }
        }
    }
}