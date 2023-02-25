namespace LightningQueues;

public class MessageContext
{
    internal MessageContext(Message message, Queue queue)
    {
        Message = message;
        QueueContext = new QueueContext(queue, message);
    }

    public Message Message { get; }
    public IQueueContext QueueContext { get; }
}