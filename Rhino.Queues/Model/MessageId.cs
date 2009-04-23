using System;

namespace Rhino.Queues.Model
{
    public class MessageId
    {
        public Guid SourceInstanceId { get; set; }
        public Guid MessageIdentifier { get; set; }

        public static MessageId GenerateRandom()
        {
            return new MessageId
            {
                SourceInstanceId = Guid.NewGuid(),
                MessageIdentifier = Guid.NewGuid()
            };
        }

        public override string ToString()
        {
            return string.Format("{0}/{1}", SourceInstanceId, MessageIdentifier);
        }
    }
}