using FubuCore.Logging;
using LightningQueues.Model;

namespace LightningQueues.Logging
{
    public class MessageReceived : LogRecord
    {
        public Message Message { get; private set; }

        public MessageReceived(Message message)
        {
            Message = message;
        }
    }
}