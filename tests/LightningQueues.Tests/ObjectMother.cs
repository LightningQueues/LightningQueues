using System;
using System.Collections.Generic;
using System.Text;

namespace LightningQueues.Tests
{
    public static class ObjectMother
    {
        public  static Message NewIncomingMessage(string queueName = "cleverqueuename", string payload = "hello", string headerValue = "myvalue")
        {
            var message = new Message
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