using System;

namespace LightningQueues.Storage;

public class QueueDoesNotExistException : Exception
{
    public QueueDoesNotExistException(string queueName, Exception? inner = null)
        : base($"Queue: {queueName} does not exist", inner)
    {
        QueueName = queueName;
    }

    public string QueueName { get; }
}