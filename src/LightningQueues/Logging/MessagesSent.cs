using FubuCore.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;

namespace LightningQueues.Logging
{
    public class MessagesSent : LogRecord
    {
        public Message[] Messages { get; private set; }
        public Endpoint Destination { get; private set; }

        public MessagesSent(Message[] messages, Endpoint destination)
        {
            Messages = messages;
            Destination = destination;
        }

        protected bool Equals(MessagesSent other)
        {
            return Messages.Equals(other.Messages) && Destination.Equals(other.Destination);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((MessagesSent) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Messages.GetHashCode()*397) ^ Destination.GetHashCode();
            }
        }
    }
}