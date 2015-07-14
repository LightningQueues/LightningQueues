using System;
using System.Collections.Generic;
using System.IO;

namespace LightningQueues.Net
{
    public class OutgoingMessageBatch
    {
        public OutgoingMessageBatch(Uri destination, IEnumerable<OutgoingMessage> messages)
        {
            Destination = destination;
            var messagesList = new List<OutgoingMessage>();
            messagesList.AddRange(messages);
            Messages = messagesList;
        }

        public Uri Destination { get; set; }
        public Stream Stream { get; set; }
        public IList<OutgoingMessage> Messages { get; }
    }
}