using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Model;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    public class CanStreamMessages
    {
        private QueueManager _sender;
        private QueueManager _receiver;

        [SetUp]
        public void Setup()
        {
            _sender = ObjectMother.QueueManager();
            _sender.Start();

            _receiver = ObjectMother.QueueManager("test2", 23457);
            _receiver.CreateQueues("a");
            _receiver.Start();
        }

        [TearDown]
        public void Teardown()
        {
            _sender.Dispose();
            _receiver.Dispose();
        }

        [Test]
        public void CanReceiveSingleMessageInAStream()
        {
            var handle = new ManualResetEvent(false);
            byte[] data = null;
            ThreadPool.QueueUserWorkItem(_ =>
            {
                var messages = _receiver.ReceiveStream("h", null);
                messages.Each(x =>
                {
                    data = x.Message.Data;
                    x.TransactionalScope.Commit();
                    handle.Set();
                });
            });
            using (var tx = new TransactionScope())
            {
                _sender.Send(new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {1, 2, 4, 5}
                    });
                tx.Complete();
            }

            handle.WaitOne(TimeSpan.FromSeconds(3));
            new byte[] {1, 2, 4, 5}.ShouldEqual(data);
        }

        [Test]
        public void CanReceiveSeveralMessagesInAStreamConcurrently()
        {
            var received = new ConcurrentBag<Message>();

            ThreadPool.QueueUserWorkItem(_ =>
            {
                var messages = _receiver.ReceiveStream("h", null);
                Parallel.ForEach(messages, new ParallelOptions {MaxDegreeOfParallelism = 4}, x =>
                {
                    received.Add(x.Message);
                    x.TransactionalScope.Commit();
                });
            });

            for (int i = 0; i < 20; ++i)
            {
                var scope = _sender.BeginTransactionalScope();
                scope.Send(new Uri("rhino.queues://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = new byte[] {(byte) i, 2, 4, 5}
                    });
                scope.Commit();
            }

            Wait.Until(() => received.Count == 20).ShouldBeTrue();
        }
    }
}