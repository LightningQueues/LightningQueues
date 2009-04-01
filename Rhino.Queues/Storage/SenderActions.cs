using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
    public class SenderActions : AbstractActions
    {
        private readonly Guid instanceId;

        public SenderActions(JET_INSTANCE instance, string database, Guid instanceId)
            : base(instance, database)
        {
            this.instanceId = instanceId;
        }

        public IList<PersistentMessage> GetMessagesToSendAndMarkThemAsInFlight(int maxNumberOfMessage, int maxSizeOfMessagesInTotal, out Endpoint endPoint)
        {
            Api.MoveBeforeFirst(session, outgoing);

            endPoint = null;
            var messages = new List<PersistentMessage>();

            while (Api.TryMoveNext(session, outgoing))
            {
                var value = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["send_status"]).Value;
                if (value != OutgoingMessageStatus.Ready)
                    continue;

                var timeAsDate = Api.RetrieveColumnAsDouble(session, outgoing, outgoingColumns["time_to_send"]).Value;
                var time = DateTime.FromOADate(timeAsDate);
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

                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                messages.Add(new PersistentMessage
                {
                    Id = new MessageId
                    {
                        Guid = instanceId,
                        Number = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value
                    },
                    Queue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["queue"], Encoding.Unicode),
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
                        (int)OutgoingMessageStatus.Ready);

                    update.Save();
                }
                Api.JetDelete(session, outgoing);
            }
        }

        public void MarkOutgoingMessageAsSuccessfullySent(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
            {
                foreach (var column in outgoingColumns.Keys)
                {
                    Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns[column],
                        Api.RetrieveColumn(session, outgoing, outgoingColumns[column])
                        );
                }
                Api.SetColumn(session, outgoingHistory, outgoingHistoryColumns["send_status"],
                              (int)OutgoingMessageStatus.Sent);

                update.Save();
            }
            Api.JetDelete(session, outgoing);
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
                        Guid = instanceId,
                        Number = Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoing, outgoingColumns["queue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, outgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, outgoingColumns["data"]),
                    Bookmark = bookmark
                };
            }
        }

        public IEnumerable<PersistentMessageToSend> GetSentMessages()
        {
            Api.MoveBeforeFirst(session, outgoingHistory);

            while (Api.TryMoveNext(session, outgoingHistory))
            {
                var address = Api.RetrieveColumnAsString(session, outgoingHistory, outgoingHistoryColumns["address"]);
                var port = Api.RetrieveColumnAsInt32(session, outgoingHistory, outgoingHistoryColumns["port"]).Value;

                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoingHistory, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                yield return new PersistentMessageToSend
                {
                    Id = new MessageId
                    {
                        Guid = instanceId,
                        Number = Api.RetrieveColumnAsInt32(session, outgoingHistory, outgoingHistoryColumns["msg_id"]).Value
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoingHistory, outgoingHistoryColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoingHistory, outgoingHistoryColumns["queue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoingHistory, outgoingHistoryColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoingHistory, outgoingHistoryColumns["data"]),
                    Bookmark = bookmark
                };
            }
            
        }
    }
}