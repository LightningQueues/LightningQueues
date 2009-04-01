using System;
using System.IO;
using Rhino.Queues.Model;

namespace Rhino.Queues.Protocol
{
    public static class SerializationExtensions
    {
        public static Message[] ToMessages(byte[] buffer)
        {
            using(var ms = new MemoryStream(buffer))
            using (var br = new BinaryReader(ms))
            {
                var numberOfMessages = br.ReadInt32();
                var msgs = new Message[numberOfMessages];
                for (int i = 0; i < numberOfMessages; i++)
                {
                    msgs[i] = new Message
                    {
                        Id = new MessageId
                        {
                            Guid = new Guid(br.ReadBytes(16)),
                            Number = br.ReadInt32()
                        },
                        Queue = br.ReadString(),
                        SubQueue = br.ReadString(),
                        SentAt = DateTime.FromBinary(br.ReadInt64()),
                    };
                    var byteCount = br.ReadInt32();
                    msgs[i].Data = br.ReadBytes(byteCount);
                    if(string.IsNullOrEmpty(msgs[i].SubQueue))
                        msgs[i].SubQueue = null;
                }
                return msgs;
            }
        }

        public static byte[] Serialize(this Message[] messages)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(messages.Length);
                foreach (var message in messages)
                {
                    writer.Write(message.Id.Guid.ToByteArray());
                    writer.Write(message.Id.Number);
                    writer.Write(message.Queue);
                    writer.Write(message.SubQueue ?? "");
                    writer.Write(message.SentAt.ToBinary());
                    writer.Write(message.Data.Length);
                    writer.Write(message.Data);
                }
                writer.Flush();
                return stream.ToArray();
            }
        }
        
    }
}