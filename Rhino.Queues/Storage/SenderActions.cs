using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using Common.Logging;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
    public class SenderActions : AbstractActions
    {
        private readonly ILog logger = LogManager.GetLogger(typeof (SenderActions));

		public SenderActions(JET_INSTANCE instance, ColumnsInformation columnsInformation, string database, Guid instanceId)
            : base(instance, columnsInformation, database, instanceId)
        {
        }

        public IList<PersistentMessage> GetMessagesToSendAndMarkThemAsInFlight(int maxNumberOfMessage, int maxSizeOfMessagesInTotal, out Endpoint endPoint)
        {
            Api.MoveBeforeFirst(session, outgoing);

            endPoint = null;
        	string queue = null;
            var messages = new List<PersistentMessage>();

            while (Api.TryMoveNext(session, outgoing))
            {
				var msgId = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]));
                var value = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"]).Value;
                var timeAsDate = Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["time_to_send"]).Value;
                var time = DateTime.FromOADate(timeAsDate);

                logger.DebugFormat("Scanning message {0} with status {1} to be sent at {2}", msgId, value, time);
                if (value != OutgoingMessageStatus.Ready)
                    continue;

                // Check if the message has expired, and move it to the outgoing history.
                var deliverBy = Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["deliver_by"]);
                if (deliverBy != null)
                {
                    var deliverByTime = DateTime.FromOADate(deliverBy.Value);
                    if (deliverByTime < DateTime.Now)
                    {
                        logger.InfoFormat("Outgoing message {0} was not succesfully sent by its delivery time limit {1}", msgId, deliverByTime);
                        var numOfRetries = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"]).Value;
                        MoveFailedMessageToOutgoingHistory(numOfRetries, msgId);
                        continue;
                    }
                }

                var maxAttempts = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["max_attempts"]);
                if (maxAttempts != null)
                {
                    var numOfRetries = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"]).Value;
                    if (numOfRetries > maxAttempts)
                    {
                        logger.InfoFormat("Outgoing message {0} has reached its max attempts of {1}", msgId, maxAttempts);
                        MoveFailedMessageToOutgoingHistory(numOfRetries, msgId);
                        continue;
                    }
                }

                if (time > DateTime.Now)
                    continue;

                var rowEndpoint = new Endpoint(
                    Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["address"]),
                    Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["port"]).Value
                    );

                if (endPoint == null)
                    endPoint = rowEndpoint;

                if (endPoint.Equals(rowEndpoint) == false)
                    continue;

				var rowQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["queue"], Encoding.Unicode);

				if (queue == null) 
					queue = rowQueue;

				if(queue != rowQueue)
					continue;
                
                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                logger.DebugFormat("Adding message {0} to returned messages", msgId);
                var headerAsQueryString = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["headers"],Encoding.Unicode);
            	messages.Add(new PersistentMessage
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
                        MessageIdentifier = msgId
                    },
                    Headers = HttpUtility.ParseQueryString(headerAsQueryString),
                    Queue = rowQueue,
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["data"]),
                    Bookmark = bookmark
                });

                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"],
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
            var numOfRetries = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"]).Value;
            var msgId = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]));

            if (numOfRetries < 100 && queueDoesNotExistsInDestination == false)
            {
                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    var timeToSend = DateTime.Now.AddSeconds(numOfRetries * numOfRetries);


                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"], (int)OutgoingMessageStatus.Ready);
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["time_to_send"],
                                  timeToSend.ToOADate());
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"],
                                  numOfRetries + 1);

                    logger.DebugFormat("Marking outgoing message {0} as failed with retries: {1}",
                                       msgId, numOfRetries);

                    update.Save();
                }
            }
            else
            {
                MoveFailedMessageToOutgoingHistory(numOfRetries, msgId);
            }
        }

        public MessageBookmark MarkOutgoingMessageAsSuccessfullySent(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            var newBookmark = new MessageBookmark();
            using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
            {
                foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                {
                	var bytes = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column]);
					Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column], bytes);
                }
				Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["send_status"],
                              (int)OutgoingMessageStatus.Sent);

                update.Save(newBookmark.Bookmark, newBookmark.Size, out newBookmark.Size);
            }
            var msgId = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]));
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
                var address = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["address"]);
                var port = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["port"]).Value;

                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                yield return new PersistentMessageToSend
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
						MessageIdentifier = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]))
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["queue"], Encoding.Unicode),
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["data"]),
                    Bookmark = bookmark
                };
            }
        }

        public void RevertBackToSend(MessageBookmark[] bookmarks)
        {
            foreach (var bookmark in bookmarks)
            {
                Api.JetGotoBookmark(session, outgoingHistory, bookmark.Bookmark, bookmark.Size);
                var msgId = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]));

                using(var update = new  Update(session, outgoing, JET_prep.Insert))
                {
                    foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                    {
                        Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column],
							Api.RetrieveColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column])
                            );
                    }
					Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"],
						(int)OutgoingMessageStatus.Ready);
					Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"],
						Api.RetrieveColumnAsInt32(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["number_of_retries"]).Value + 1
						   );

                    logger.DebugFormat("Reverting output message {0} back to Ready mode", msgId);

                    update.Save();
                }
            }
        }

        private void MoveFailedMessageToOutgoingHistory(int numOfRetries, Guid msgId)
        {
            using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
            {
                foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                {
                    Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column],
                        Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column])
                        );
                }
                Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["send_status"],
                    (int)OutgoingMessageStatus.Failed);

                logger.DebugFormat("Marking outgoing message {0} as permenantly failed after {1} retries",
                    msgId, numOfRetries);

                update.Save();
            }
            Api.JetDelete(session, outgoing);
        }
    }
}