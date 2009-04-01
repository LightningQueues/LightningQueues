using System;

namespace Rhino.Queues.Model
{
    public class MessageId
    {
        public Guid Guid { get; set; }
        public int Number { get; set; }

        public static MessageId GenerateRandom()
        {
            return new MessageId
            {
                Guid = Guid.NewGuid(),
                Number = 42
            };
        }

        public override string ToString()
        {
            return string.Format("{0}/{1}", Guid, Number);
        }
    }
}