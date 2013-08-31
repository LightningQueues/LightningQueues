using LightningQueues.Protocol;

namespace LightningQueues.Model
{
    public class MessagesForEndpoint
    {
        public Endpoint Destination { get; set; }
        public PersistentMessage[] Messages { get; set; }
    }
}