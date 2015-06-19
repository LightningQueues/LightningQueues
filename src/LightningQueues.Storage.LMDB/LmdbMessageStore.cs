using System;
using System.Collections.Generic;
using System.Linq;
using LightningDB;

namespace LightningQueues.Storage.LMDB
{
    public class LmdbMessageStore : IMessageStore, IDisposable
    {
        private readonly LightningEnvironment _environment;

        public LmdbMessageStore(string path)
        {
            _environment = new LightningEnvironment(path) {MaxDatabases = 5};
            _environment.Open();
        }

        public LightningEnvironment Environment => _environment;

        public ITransaction StoreMessages(params IncomingMessage[] messages)
        {
            var transaction = _environment.BeginTransaction();
            var openedDatabases = new List<LightningDatabase>();
            try
            {
                foreach (var messagesByQueue in messages.GroupBy(x => x.Queue))
                {
                    var db = transaction.OpenDatabase(messagesByQueue.Key);
                    openedDatabases.Add(db);
                    foreach (var message in messagesByQueue)
                    {
                        transaction.Put(db, message.Id.ToString(), message.Data);
                        transaction.Put(db, $"{message.Id}/headers", message.Headers.ToBytes());
                        transaction.Put(db, $"{message.Id}/sent", BitConverter.GetBytes(message.SentAt.ToBinary()));
                    }
                }
            }
            catch (LightningException ex) 
            {
                transaction.Abort();
                transaction.Dispose();
                openedDatabases.CloseAll();
                if(ex.StatusCode == -30798) //MDB_NOTFOUND
                    throw new QueueDoesNotExistException("Queue doesn't exist", ex);
                throw;
            }
            return new LmdbTransaction(transaction, openedDatabases);
        }

        public void CreateQueue(string queueName)
        {
            using (var tx = _environment.BeginTransaction())
            {
                using (tx.OpenDatabase(queueName, new DatabaseOptions {Flags = DatabaseOpenFlags.Create}))
                {
                    tx.Commit();
                }
            }
        }

        public void Dispose()
        {
            _environment.Dispose();
        }
    }
}