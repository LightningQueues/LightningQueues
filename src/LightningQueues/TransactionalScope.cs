using System;
using LightningQueues.Internal;
using LightningQueues.Model;

namespace LightningQueues
{
    public class TransactionalScope : ITransactionalScope
    {
        private readonly ITransactionalQueueManager queueManager;
        private readonly ITransaction transaction;

        public TransactionalScope(ITransactionalQueueManager queueManager, ITransaction transaction)
        {
            this.queueManager = queueManager;
            this.transaction = transaction;
        }

        public Message Receive(string queue)
        {
            return queueManager.Receive(transaction, queue);
        }

        public Message Receive(string queue, TimeSpan timeout)
        {
            return queueManager.Receive(transaction, queue, timeout);
        }

        public Message Receive(string queue, string subqueue)
        {
            return queueManager.Receive(transaction, queue);
        }

        public Message Receive(string queue, string subqueue, TimeSpan timeout)
        {
            return queueManager.Receive(transaction, queue, subqueue, timeout);
        }

        public MessageId Send(Uri uri, MessagePayload payload)
        {
            return queueManager.Send(transaction, uri, payload);
        }

        public void Commit()
        {
            transaction.Commit();
        }

        public void Rollback()
        {
            transaction.Rollback();
        }
    }
}