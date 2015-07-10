using System;

namespace LightningQueues
{
    public class OutgoingMessage : Message
    {
        public Uri Destination { get; set; }
        public DateTime? DeliverBy { get; set; }
        public int? MaxAttempts { get; set; }
    }
}