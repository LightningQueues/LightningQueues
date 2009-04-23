using Rhino.Queues.Storage;

namespace Rhino.Queues.Model
{
    public class PersistentMessage : Message
    {
        public MessageBookmark Bookmark { get; set; }
        public MessageStatus Status { get; set; }
    }
}