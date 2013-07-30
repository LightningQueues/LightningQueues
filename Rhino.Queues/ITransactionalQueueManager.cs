using System;
using Rhino.Queues.Internal;
using Rhino.Queues.Model;

namespace Rhino.Queues
{
    public interface ITransactionalQueueManager
    {
        Message Receive(ITransaction transaction, string queueName);
        Message Receive(ITransaction transaction, string queueName, TimeSpan timeout);
        Message Receive(ITransaction transaction, string queueName, string subqueue);
        Message Receive(ITransaction transaction, string queueName, string subqueue, TimeSpan timeout);
        MessageId Send(ITransaction transaction, Uri uri, MessagePayload payload);
    }
}