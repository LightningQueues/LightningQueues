using Microsoft.Isam.Esent.Interop;

namespace Rhino.Queues.Storage
{
    public class MessageBookmark
    {
        public MessageBookmark()
        {
            
        }
        public string QueueName;
        public byte[] Bookmark = new byte[SystemParameters.BookmarkMost];
        public int Size = SystemParameters.BookmarkMost;
    }
}