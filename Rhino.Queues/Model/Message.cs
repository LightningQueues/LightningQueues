using System;
using Rhino.Queues.Storage;

namespace Rhino.Queues.Model
{
    public class Message
    {
        public MessageId Id { get; set; }
        public string Queue { get; set; }
        public DateTime SentAt { get; set; }
        public byte[] Data { get; set; }
    }
}