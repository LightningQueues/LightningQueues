using System;

namespace Rhino.Queues.Model
{
    public class HistoryMessage : PersistentMessage
    {
        public DateTime MovedToHistoryAt { get; set; }
    }
}