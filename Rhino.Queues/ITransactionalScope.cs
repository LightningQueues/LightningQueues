using System;
using Rhino.Queues.Model;

namespace Rhino.Queues
{
    public interface ITransactionalScope
    {
        Message Receive(string queue);
        Message Receive(string queue, TimeSpan timeout);
        Message Receive(string queue, string subqueue);
        Message Receive(string queue, string subqueue, TimeSpan timeout);
        MessageId Send(Uri uri, MessagePayload payload);
        void Commit();
        void Rollback();
    }
}