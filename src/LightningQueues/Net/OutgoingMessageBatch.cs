using System;
using System.Collections.Generic;
using System.IO;

namespace LightningQueues.Net
{
    public class OutgoingMessageBatch
    {
        public OutgoingMessageBatch(Uri destination)
        {
            Destination = destination;
            Messages = new List<OutgoingMessage>();
        }

        public Uri Destination { get; set; }
        public Stream Stream { get; set; }
        public IList<OutgoingMessage> Messages { get; set; }

        public OutgoingMessageBatch AddMessage(OutgoingMessage message)
        {
            Messages.Add(message);
            return this;
        }
    }
}