using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
	using Utils;

	public class GlobalActions : AbstractActions
    {
        private readonly Guid instanceId;
        private readonly ILog logger = LogManager.GetLogger(typeof(GlobalActions));

        public GlobalActions(JET_INSTANCE instance, string database, Guid instanceId)
            : base(instance, database)
        {
            this.instanceId = instanceId;
        }

        public void CreateQueueIfDoesNotExists(string queueName)
        {
            Api.MakeKey(session, queues, queueName, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, queues, SeekGrbit.SeekEQ))
                return;

            new QueueSchemaCreator(session, dbid, queueName).Create();
            using (var updateQueue = new Update(session, queues, JET_prep.Insert))
            {
                Api.SetColumn(session, queues, queuesColumns["name"], queueName, Encoding.Unicode);
                Api.SetColumn(session, queues, queuesColumns["created_at"], DateTime.Now.ToOADate());
                updateQueue.Save();
            }
        }

        public void RegisterRecoveryInformation(Guid transactionId, byte[] information)
        {
            using (var update = new Update(session, recovery, JET_prep.Insert))
            {
                Api.SetColumn(session, recovery, recoveryColumns["tx_id"], transactionId.ToByteArray());
                Api.SetColumn(session, recovery, recoveryColumns["recovery_info"], information);

                update.Save();
            }
        }

        public void DeleteRecoveryInformation(Guid transactionId)
        {
            Api.MakeKey(session, recovery, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);

            if (Api.TrySeek(session, recovery, SeekGrbit.SeekEQ) == false)
                return;
            Api.JetDelete(session, recovery);
        }

        public IEnumerable<byte[]> GetRecoveryInformation()
        {
            Api.MoveBeforeFirst(session, recovery);
            while (Api.TryMoveNext(session, recovery))
            {
                yield return Api.RetrieveColumn(session, recovery, recoveryColumns["recovery_info"]);
            }
        }

        public void RegisterUpdateToReverse(Guid txId, MessageBookmark bookmark, MessageStatus statusToRestore, string subQueue)
        {
            using (var update = new Update(session, txs, JET_prep.Insert))
            {
                Api.SetColumn(session, txs, txsColumns["tx_id"], txId.ToByteArray());
                Api.SetColumn(session, txs, txsColumns["bookmark_size"], bookmark.Size);
                Api.SetColumn(session, txs, txsColumns["bookmark_data"], bookmark.Bookmark.Take(bookmark.Size).ToArray());
                Api.SetColumn(session, txs, txsColumns["value_to_restore"], (int)statusToRestore);
                Api.SetColumn(session, txs, txsColumns["queue"], bookmark.QueueName, Encoding.Unicode);
                Api.SetColumn(session, txs, txsColumns["subqueue"], subQueue, Encoding.Unicode);

                update.Save();
            }
        }

        public void RemoveReversalsMoveCompletedMessagesAndFinishSubQueueMove(Guid transactionId)
        {
            Api.JetSetCurrentIndex(session, txs, "by_tx_id");
            Api.MakeKey(session, txs, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);

            if (Api.TrySeek(session, txs, SeekGrbit.SeekEQ) == false)
                return;
            Api.MakeKey(session, txs, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
        	try
        	{
        		Api.JetSetIndexRange(session, txs, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
        	}
        	catch (EsentErrorException e)
        	{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
        		return;
        	}

            do
            {
                var queue = Api.RetrieveColumnAsString(session, txs, txsColumns["queue"], Encoding.Unicode);
                var bookmarkData = Api.RetrieveColumn(session, txs, txsColumns["bookmark_data"]);
                var bookmarkSize = Api.RetrieveColumnAsInt32(session, txs, txsColumns["bookmark_size"]).Value;

                var actions = GetQueue(queue);

                var bookmark = new MessageBookmark
                {
                    Bookmark = bookmarkData,
                    QueueName = queue,
                    Size = bookmarkSize
                };

                switch (actions.GetMessageStatus(bookmark))
                {
                    case MessageStatus.SubqueueChanged:
                    case MessageStatus.EnqueueWait:
                        actions.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                        actions.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                        break;
                    default:
                        actions.MoveToHistory(bookmark);
                        break;
                }
                    

                Api.JetDelete(session, txs);
            } while (Api.TryMoveNext(session, txs));
        }

        public Guid RegisterToSend(Endpoint destination, string queue, string subQueue, MessagePayload payload, Guid transactionId)
        {
            var bookmark = new MessageBookmark();
			var msgId = GuidCombGenerator.Generate();
			using (var update = new Update(session, outgoing, JET_prep.Insert))
            {
            	Api.SetColumn(session, outgoing, outgoingColumns["msg_id"], msgId.ToByteArray());
				Api.SetColumn(session, outgoing, outgoingColumns["tx_id"], transactionId.ToByteArray());
                Api.SetColumn(session, outgoing, outgoingColumns["address"], destination.Host, Encoding.Unicode);
                Api.SetColumn(session, outgoing, outgoingColumns["port"], destination.Port);
                Api.SetColumn(session, outgoing, outgoingColumns["time_to_send"], DateTime.Now.ToOADate());
                Api.SetColumn(session, outgoing, outgoingColumns["sent_at"], DateTime.Now.ToOADate());
                Api.SetColumn(session, outgoing, outgoingColumns["send_status"], (int)OutgoingMessageStatus.NotReady);
                Api.SetColumn(session, outgoing, outgoingColumns["queue"], queue, Encoding.Unicode);
                Api.SetColumn(session, outgoing, outgoingColumns["subqueue"], subQueue, Encoding.Unicode);
                Api.SetColumn(session, outgoing, outgoingColumns["headers"], payload.Headers.ToQueryString(),
                              Encoding.Unicode);
                Api.SetColumn(session, outgoing, outgoingColumns["data"], payload.Data);
                Api.SetColumn(session, outgoing, outgoingColumns["number_of_retries"], 1);
                Api.SetColumn(session, outgoing, outgoingColumns["size_of_data"], payload.Data.Length);

                update.Save(bookmark.Bookmark, bookmark.Size, out bookmark.Size);
            }
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            logger.DebugFormat("Created output message '{0}' for 'rhino.queues://{1}:{2}/{3}/{4}' as NotReady",
                msgId,
                destination.Host,
                destination.Port,
                queue,
                subQueue
                );
            return msgId;
        }

        public void MarkAsReadyToSend(Guid transactionId)
        {
            Api.JetSetCurrentIndex(session, outgoing, "by_tx_id");

            Api.MakeKey(session, outgoing, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, outgoing, SeekGrbit.SeekEQ) == false)
                return;
            Api.MakeKey(session, outgoing, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
        	try
        	{
        		Api.JetSetIndexRange(session, outgoing,
        		                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
        	}
        	catch (EsentErrorException e)
        	{
				if (e.Error!=JET_err.NoCurrentRecord)
					throw;
        		return;
        	}
            do
            {
                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    Api.SetColumn(session, outgoing, outgoingColumns["send_status"], (int)OutgoingMessageStatus.Ready);

                    update.Save();
                }
                logger.DebugFormat("Marking output message {0} as Ready",
                    Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value);
            } while (Api.TryMoveNext(session, outgoing));
        }

        public void DeleteMessageToSend(Guid transactionId)
        {
            Api.JetSetCurrentIndex(session, outgoing, "by_tx_id");

            Api.MakeKey(session, outgoing, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
            if (Api.TrySeek(session, outgoing, SeekGrbit.SeekEQ) == false)
                return;
            Api.MakeKey(session, outgoing, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
        	try
        	{
        		Api.JetSetIndexRange(session, outgoing,
        		                     SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
        	}
        	catch (EsentErrorException e)
        	{
				if (e.Error!=JET_err.NoCurrentRecord)
					throw;
        		return;
        	}
        	do
            {
                logger.DebugFormat("Deleting output message {0}",
                    Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["msg_id"]).Value);
                Api.JetDelete(session, outgoing);
            } while (Api.TryMoveNext(session, outgoing));
        }

        public void MarkAllOutgoingInFlightMessagesAsReadyToSend()
        {
            Api.MoveBeforeFirst(session, outgoing);
            while (Api.TryMoveNext(session, outgoing))
            {
                var status = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, outgoingColumns["send_status"]).Value;
                if (status != OutgoingMessageStatus.InFlight)
                    continue;

                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    Api.SetColumn(session, outgoing, outgoingColumns["send_status"], (int)OutgoingMessageStatus.Ready);

                    update.Save();
                }
            }
        }

        public void MarkAllProcessedMessagesWithTransactionsNotRegisterForRecoveryAsReadyToDeliver()
        {
            var txsWithRecovery = new HashSet<Guid>();
            Api.MoveBeforeFirst(session, recovery);
            while (Api.TryMoveNext(session, recovery))
            {
                var idAsBytes = Api.RetrieveColumn(session, recovery, recoveryColumns["tx_id"]);
                txsWithRecovery.Add(new Guid(idAsBytes));
            }

            var txsWithoutRecovery = new HashSet<Guid>();
            Api.MoveBeforeFirst(session, txs);
            while (Api.TryMoveNext(session, txs))
            {
                var idAsBytes = Api.RetrieveColumn(session, txs, recoveryColumns["tx_id"]);
                txsWithoutRecovery.Add(new Guid(idAsBytes));
            }

            foreach (var txId in txsWithoutRecovery)
            {
                if (txsWithRecovery.Contains(txId))
                    continue;
                ReverseAllFrom(txId);
            }
        }

        public void ReverseAllFrom(Guid transactionId)
        {
            Api.JetSetCurrentIndex(session, txs, "by_tx_id");
            Api.MakeKey(session, txs, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);

            if (Api.TrySeek(session, txs, SeekGrbit.SeekEQ) == false)
                return;

            Api.MakeKey(session, txs, transactionId.ToByteArray(), MakeKeyGrbit.NewKey);
        	try
        	{
        		Api.JetSetIndexRange(session, txs, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
        	}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.NoCurrentRecord)
					throw;
				return;
			}

            do
            {
                var bytes = Api.RetrieveColumn(session, txs, txsColumns["bookmark_data"]);
                var size = Api.RetrieveColumnAsInt32(session, txs, txsColumns["bookmark_size"]).Value;
                var oldStatus = (MessageStatus)Api.RetrieveColumnAsInt32(session, txs, txsColumns["value_to_restore"]).Value;
                var queue = Api.RetrieveColumnAsString(session, txs, txsColumns["queue"]);
                var subqueue = Api.RetrieveColumnAsString(session, txs, txsColumns["subqueue"]);

                var bookmark = new MessageBookmark
                {
                    QueueName = queue,
                    Bookmark = bytes,
                    Size = size
                };
                var actions = GetQueue(queue);
                var newStatus = actions.GetMessageStatus(bookmark);
                switch (newStatus)
                {
                    case MessageStatus.SubqueueChanged:
                        actions.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver, subqueue);
                        break;
                    case MessageStatus.EnqueueWait:
                        actions.Delete(bookmark);
                        break;
                    default:
                        actions.SetMessageStatus(bookmark, oldStatus);
                        break;
                }
            } while (Api.TryMoveNext(session, txs));
        }

        public string[] GetAllQueuesNames()
        {
            var names = new List<string>();
            Api.MoveBeforeFirst(session, queues);
            while (Api.TryMoveNext(session, queues))
            {
                names.Add(Api.RetrieveColumnAsString(session, queues, queuesColumns["name"]));
            }
            return names.ToArray();
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
                        SourceInstanceId = instanceId,
                        MessageIdentifier = new Guid(Api.RetrieveColumn(session, outgoingHistory, outgoingHistoryColumns["msg_id"]))
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoingHistory, outgoingHistoryColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoingHistory, outgoingHistoryColumns["queue"], Encoding.Unicode),
                    SubQueue = Api.RetrieveColumnAsString(session, outgoingHistory, outgoingHistoryColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoingHistory, outgoingHistoryColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoingHistory, outgoingHistoryColumns["data"]),
                    Bookmark = bookmark
                };
            }

        }

        public void DeleteMessageToSendHistoric(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, outgoingHistory, bookmark.Bookmark, bookmark.Size);
            Api.JetDelete(session, outgoingHistory);
        }

    	public int GetNumberOfMessages(string queueName)
    	{
			Api.JetSetCurrentIndex(session, queues, "pk");
			Api.MakeKey(session, queues, queueName, Encoding.Unicode, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, queues, SeekGrbit.SeekEQ) == false)
				return -1;

    		var bytes = new byte[4];
    		var zero = BitConverter.GetBytes(0);
    		int actual;
    		Api.JetEscrowUpdate(session, queues, queuesColumns["number_of_messages"],
    		                    zero, zero.Length, bytes, bytes.Length, out actual, EscrowUpdateGrbit.None);
    		return BitConverter.ToInt32(bytes, 0);
    	}

		public IEnumerable<MessageId> GetAlreadyReceivedMessageIds()
		{
			Api.MoveBeforeFirst(session, recveivedMsgs);
			while(Api.TryMoveNext(session, recveivedMsgs))
			{
				yield return new MessageId
				{
					SourceInstanceId = new Guid(Api.RetrieveColumn(session, recveivedMsgs, recveivedMsgsColumns["instance_id"])),
					MessageIdentifier = new Guid(Api.RetrieveColumn(session, recveivedMsgs, recveivedMsgsColumns["msg_id"])),
				};
			}
		}

		public void MarkReceived(MessageId id)
		{
			using(var update = new Update(session, recveivedMsgs, JET_prep.Insert))
			{
                Api.SetColumn(session, recveivedMsgs, recveivedMsgsColumns["instance_id"], id.SourceInstanceId.ToByteArray());
				Api.SetColumn(session, recveivedMsgs, recveivedMsgsColumns["msg_id"], id.MessageIdentifier.ToByteArray());

				update.Save();
			}
		}

		public IEnumerable<MessageId> DeleteOldestReceivedMessages(int numberOfItemsToKeep)
		{
			Api.MoveAfterLast(session, recveivedMsgs);
			try
			{
				Api.JetMove(session, recveivedMsgs, -numberOfItemsToKeep, MoveGrbit.None);
			}
			catch (EsentErrorException e)
			{
				if (e.Error == JET_err.NoCurrentRecord)
					yield break;
				throw;
			}
			while(Api.TryMovePrevious(session, recveivedMsgs))
			{
				yield return new MessageId
				{
					SourceInstanceId = new Guid(Api.RetrieveColumn(session, recveivedMsgs, recveivedMsgsColumns["instance_id"])),
					MessageIdentifier = new Guid(Api.RetrieveColumn(session, recveivedMsgs, recveivedMsgsColumns["msg_id"])),
				}; 
				Api.JetDelete(session, recveivedMsgs);
			}
		}
    }
}
