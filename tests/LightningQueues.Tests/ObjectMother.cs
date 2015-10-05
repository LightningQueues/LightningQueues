using System;
using LightningQueues.Storage.LMDB;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Text;
using LightningQueues.Logging;

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

        public static Queue NewLmdbQueue(string path, string queueName = "test", ILogger logger = null, IScheduler scheduler = null)
        {
            var queueConfiguration = new QueueConfiguration();
            queueConfiguration.LogWith(logger ?? new RecordingLogger());
            queueConfiguration.AutomaticEndpoint();
            queueConfiguration.ScheduleQueueWith(scheduler ?? TaskPoolScheduler.Default);
            queueConfiguration.StoreWithLmdb(path);
            var queue = queueConfiguration.BuildQueue();
            queue.CreateQueue(queueName);
            queue.Start();
            return queue;
        }
    }
}