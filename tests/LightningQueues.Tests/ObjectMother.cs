using System;
using System.Collections.Generic;
using System.Text;

namespace LightningQueues.Tests
{
    public static class ObjectMother
    {
        public  static T NewMessage<T>(string queueName = "cleverqueuename", string payload = "hello", string headerValue = "myvalue") where T : Message, new()
        {
            var message = new T
            {
                Data = Encoding.UTF8.GetBytes(payload),
                Headers = new Dictionary<string, string>
                {
                    {"mykey", headerValue}
                },
                Id = MessageId.GenerateRandom(),
                Queue = queueName,
                SentAt = DateTime.Now
            };
            return message;

        }
    }
}