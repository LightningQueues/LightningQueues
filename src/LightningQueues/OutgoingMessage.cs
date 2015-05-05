using System;
using System.Collections.Generic;

namespace LightningQueues
{
    public class OutgoingMessage
    {
        public OutgoingMessage()
        {
            Headers = new Dictionary<string, string[]>();
        }

        public byte[] Data { get; set; }
        public DateTime? DeliverBy { get; set; }
        public IDictionary<string, string[]> Headers { get; set; }
        public int? MaxAttempts { get; set; }
    }
}