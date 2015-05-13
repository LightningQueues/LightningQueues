using System.IO;

namespace LightningQueues.Net
{
    public class OutgoingMessageBatch
    {
        public Stream Stream { get; set; }
        public IncomingMessage[] Messages { get; set; }
    }
}