using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LightningQueues.Storage
{
    public class MessageStore<T> : IMessageStore where T : IStorage, new()
    {
        public MessageStore() : this(new T())
        {
        }

        public MessageStore(IStorage storage)
        {
            Storage = storage;
            StartRecovery();
            CreateKeyLayout();
        }

        private void CreateKeyLayout()
        {
            if (Storage.Get("q") == null)
            {
                Storage.Put("q", new byte[]{});
            }
        }

        //For testing
        public IStorage Storage { get; }

        public void CreateQueue(string queue)
        {
            Storage.Put($"q/{queue}/msgs", 
                BitConverter.GetBytes(DateTime.UtcNow.ToBinary()));
        }

        public IncomingMessage GetMessageById(string queue, MessageId id)
        {
            var key = $"/q/{queue}/msgs";
            var idString = id.ToString();
            //This is dumb, but good enough for now and probably good enough for tests
            var enumerator = Storage.GetEnumerator();
            Console.WriteLine($"Initial Key: {key}");
            while (enumerator.MoveNext())
            {
                if (enumerator.Current.Key.Contains(idString))
                {
                    key = enumerator.Current.Key;
                    Console.WriteLine($"Key found: {key}");
                    break;
                }
            }
            var message = new IncomingMessage
            {
                Data = Storage.Get(key),
                Headers = Encoding.UTF8.GetString(Storage.Get($"{key}/headers")).ParseQueryString(),
                Id = id,
                Queue = queue,
                SentAt = DateTime.FromBinary(BitConverter.ToInt64(Storage.Get($"{key}/sent"), 0))
            };
            return message;
        }

        public ITransaction StoreMessages(params IncomingMessage[] messages)
        {
            var transaction = new StorageTransaction(Storage);
            try
            {
                foreach (var message in messages)
                {
                    StoreMessage(transaction, message);
                }
            }
            catch (Exception)
            {
                transaction.Rollback();
                throw;
            }
            return transaction;
        }

        private void StoreMessage(ITransaction transaction, IncomingMessage message)
        {
            var queue = Storage.Get($"q/{message.Queue}/msgs");
            if(queue == null)
                throw new QueueDoesNotExistException($"Queue with name '{message.Queue}' doesn't exist.");

            var key = $"/q/{message.Queue}/msgs/{message.Id}/batch/{transaction.TransactionId}";
            transaction.Put(key, message.Data);
            transaction.Put($"{key}/headers", Encoding.UTF8.GetBytes(message.Headers.ToQueryString()));
            transaction.Put($"{key}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
        }

        private void StartRecovery()
        {
            var removeTxs = new List<string>();
            removeTxs.AddRange(GatherUncommittedTransactions());
            var removeMsgs = new List<string>();
            removeMsgs.AddRange(GatherMessagesUncommitted(removeTxs));
            foreach (var key in removeMsgs.Union(removeTxs))
            {
                Storage.Delete(key);
            }
        }

        private IEnumerable<string> GatherUncommittedTransactions()
        {
            var transactionEnumerator = Storage.GetEnumerator("batch");
            while (transactionEnumerator.MoveNext())
            {
                yield return transactionEnumerator.Current.Key;
            }
        }

        private IEnumerable<string> GatherMessagesUncommitted(List<string> transactions)
        {
            var msgsEnumerator = Storage.GetEnumerator("q");
            while (msgsEnumerator.MoveNext())
            {
                if (transactions.Any(x => msgsEnumerator.Current.Key.Contains(x)))
                {
                    yield return msgsEnumerator.Current.Key;
                }
            }
        } 
    }
}