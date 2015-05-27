using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LightningQueues.Storage;
using LightningQueues.Storage.InMemory;
using Should;
using Xunit;

namespace LightningQueues.Tests.Storage.InMemory
{
    public class IncomingMessageScenarios
    {
        [Fact]
        public void happy_path_success()
        {
            var store = new MessageStore<InMemoryStorage>();
            var message = newMessage();
            store.CreateQueue(message.Queue);
            store.StoreMessages(message).Commit();
            var result = store.GetMessageById(message.Queue, message.Id);
            result.Queue.ShouldEqual(message.Queue);
            result.Id.ShouldEqual(message.Id);
            Encoding.UTF8.GetString(result.Data).ShouldEqual("hello");
            result.Headers.First().Value.ShouldEqual("myvalue");
        }

        [Fact]
        public void storing_message_for_queue_that_doesnt_exist()
        {
            var store = new MessageStore<InMemoryStorage>();
            var message = newMessage();
            Assert.Throws<QueueDoesNotExistException>(() => store.StoreMessages(message));
        }

        [Fact]
        public void crash_before_commit()
        {
            var store = new MessageStore<InMemoryStorage>();
            var message = newMessage();
            store.CreateQueue(message.Queue);
            store.StoreMessages(message);
            //crash
            store = new MessageStore<InMemoryStorage>(store.Storage);
            store.Storage.GetEnumerator($"/q/{message.Queue}/msgs/{message.Id}")
                .MoveNext()
                .ShouldBeFalse();
        }

        private IncomingMessage newMessage(string queueName = "cleverqueuename", string payload = "hello", string headerValue = "myvalue")
        {
            var message = new IncomingMessage
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