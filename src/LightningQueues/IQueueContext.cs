using System;

namespace LightningQueues
{
    public interface IQueueContext
    {
        void CommitChanges();
        void Send(Uri destination, Message message);
        void ReceiveLater(TimeSpan timeSpan);
        void ReceiveLater(DateTimeOffset time);
        void SuccessfullyReceived();
        void MoveTo(string queueName);
        void Enqueue(Message message);
    }
}