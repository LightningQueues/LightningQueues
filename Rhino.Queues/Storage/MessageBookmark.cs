using Microsoft.Isam.Esent.Interop;

namespace Rhino.Queues.Storage
{
    public class MessageBookmark
    {
        public MessageBookmark()
        {
            
        }
        public string QueueName;
        public byte[] Bookmark = new byte[Api.BookmarkMost];
        public int Size = Api.BookmarkMost;
    }
}