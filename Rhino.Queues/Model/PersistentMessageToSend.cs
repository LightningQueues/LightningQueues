using System.Net;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;

namespace Rhino.Queues.Model
{
    public class PersistentMessageToSend : PersistentMessage
    {
        public OutgoingMessageStatus OutgoingStatus { get; set; }
        public Endpoint Endpoint { get; set; }
    }
}