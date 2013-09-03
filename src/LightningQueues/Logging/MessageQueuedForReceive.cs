using FubuCore.Logging;
using LightningQueues.Model;

namespace LightningQueues.Logging
{
    public class MessageQueuedForReceive : LogRecord
    {
        public Message Message { get; private set; }

        public MessageQueuedForReceive(Message message)
        {
            Message = message;
        }
    }
}