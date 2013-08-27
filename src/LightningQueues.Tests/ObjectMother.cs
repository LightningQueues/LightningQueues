using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using FubuCore.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    public static class ObjectMother
    {
        public static Message[] MessageBatchSingleMessage(string message = null, string queueName = null)
        {
            return new[]{SingleMessage()};
        }

        public static Message SingleMessage(string message = "hello", string queueName = "h")
        {
            return new Message
            {
                Id = MessageId.GenerateRandom(),
                Queue = queueName,
                Data = Encoding.Unicode.GetBytes(message),
                SentAt = DateTime.Now
            };
        }

        public static Sender Sender(int port = 23456)
        {
            return new Sender(Logger())
            {
                Destination = new Endpoint("localhost", port),
                Failure = exception => Assert.False(true),
                Success = () => null,
                Messages = MessageBatchSingleMessage(),
            };
        }

        public static Uri UriFor(int port = 23457, string queue = "h")
        {
            return new Uri(string.Format("lq.tcp://localhost:{0}/{1}", port, queue));
        }

        public static MessagePayload MessagePayload()
        {
            return new MessagePayload
            {
                Data = Encoding.UTF8.GetBytes("hello")
            };
        }

        public static QueueManager QueueManager(string name = "test", int port = 23456, string queue = "h", bool delete = true)
        {
            var directory = string.Format("{0}.esent", name);
            if (delete && Directory.Exists(directory))
                Directory.Delete(directory, true);

            var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, port), directory, new QueueManagerConfiguration(), Logger());
            queueManager.CreateQueues(queue);
            return queueManager;
        }

        public static ILogger Logger()
        {
            return new Logger(Enumerable.Empty<ILogListener>(), Enumerable.Empty<ILogModifier>());
        }
    }
}