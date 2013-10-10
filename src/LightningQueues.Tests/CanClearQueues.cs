using System;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    public class CanClearQueues
    {
        private QueueManager _sender;
        private QueueManager _receiver;

        [SetUp]
        public void Setup()
        {
            _sender = ObjectMother.QueueManager();
            _receiver = ObjectMother.QueueManager("test2", 23457);
        }

        [TearDown]
        public void Teardown()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }

        [Test]
        public void ClearsOutgoingMessages()
        {
            _sender.Start();
            sendMessages();
            _sender.GetMessagesCurrentlySending().ShouldHaveCount(50);
            _sender.ClearAllMessages();
            _sender.GetMessagesCurrentlySending().ShouldHaveCount(0);
        }

        [Test]
        public void ClearsQueueMessages()
        {
            _sender.Start();
            _receiver.Start();
            sendMessages();
            Wait.Until(() => _receiver.GetAllMessages("h", null).Length == 50).ShouldBeTrue();

            var scope = _receiver.BeginTransactionalScope();
            scope.Receive("h");
            scope.Commit();
            _receiver.GetAllProcessedMessages("h").ShouldHaveCount(1);
            _receiver.ClearAllMessages();
            _receiver.GetAllMessages("h", null).ShouldHaveCount(0);
            _receiver.GetAllProcessedMessages("h").ShouldHaveCount(0);
        }

        private void sendMessages()
        {
            var scope = _sender.BeginTransactionalScope();
            for (int i = 0; i < 50; ++i)
            {
                scope.Send(new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });
            }
            scope.Commit();
        }
    }
}