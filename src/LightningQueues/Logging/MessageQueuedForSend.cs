using FubuCore.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;

namespace LightningQueues.Logging
{
    public class MessageQueuedForSend : LogRecord
    {
        public Endpoint Destination { get; private set; }
        public Message Message { get; private set; }

        public MessageQueuedForSend(Endpoint destination, Message message)
        {
            Destination = destination;
            Message = message;
        }

        protected bool Equals(MessageQueuedForSend other)
        {
            return Destination.Equals(other.Destination) && Message.Equals(other.Message);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((MessageQueuedForSend) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Destination.GetHashCode()*397) ^ Message.GetHashCode();
            }
        }
    }
}