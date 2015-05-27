using System;
using System.Collections.Generic;

namespace LightningQueues
{
    public class IncomingMessage
    {
        public IncomingMessage()
        {
            Headers = new Dictionary<string, string>();
        }

        public MessageId Id { get; set; }
        public string Queue { get; set; }
        public DateTime SentAt { get; set; }
        public IDictionary<string, string> Headers { get; set; }
        public byte[] Data { get; set; }
        public string SubQueue { get; set; }
    }
}
