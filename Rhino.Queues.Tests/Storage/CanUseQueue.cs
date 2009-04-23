using System;
using System.IO;
using Rhino.Queues.Model;
using Rhino.Queues.Storage;
using Xunit;

namespace Rhino.Queues.Tests.Storage
{
    public class CanUseQueue
    {
        public CanUseQueue()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);
        }

        [Fact]
        public void CanCreateNewQueueFactory()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();
            }
        }

        [Fact]
        public void CanPutSingleMessageInQueue()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();

                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("h");
                    actions.Commit();
                });

                MessageBookmark bookmark = null;
                var guid = Guid.NewGuid();
				var identifier = Guid.NewGuid();
				qf.Global(actions =>
                {
                	bookmark = actions.GetQueue("h").Enqueue(new Message
                    {
                        Queue = "h",
                        Data = new byte[] { 13, 12, 43, 5 },
                        SentAt = new DateTime(2004, 5, 5),
                        Id = new MessageId { SourceInstanceId = guid, MessageIdentifier = identifier }
                    });
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    actions.GetQueue("h").SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var message = actions.GetQueue("h").Dequeue(null);

                    Assert.Equal(new byte[] { 13, 12, 43, 5 }, message.Data);
					Assert.Equal(identifier, message.Id.MessageIdentifier);
                    Assert.Equal(guid, message.Id.SourceInstanceId);
                    Assert.Equal("h", message.Queue);
                    Assert.Equal(new DateTime(2004, 5, 5), message.SentAt);
                    actions.Commit();
                });
            }
        }

        [Fact]
        public void WillGetMessagesBackInOrder()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();

                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("h");
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var queue = actions.GetQueue("h");

                    var bookmark = queue.Enqueue(new Message
                    {
                        Queue = "h",
                        Id = MessageId.GenerateRandom(),
                        Data = new byte[] { 1 },
                    });

                    queue.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);

                    bookmark = queue.Enqueue(new Message
                    {
                        Queue = "h",
                        Id = MessageId.GenerateRandom(),
                        Data = new byte[] { 2 },
                    });

                    queue.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);

                    bookmark = queue.Enqueue(new Message
                    {
                        Queue = "h",
                        Id = MessageId.GenerateRandom(),
                        Data = new byte[] { 3 },
                    });

                    queue.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);

                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var m1 = actions.GetQueue("h").Dequeue(null);
                    var m2 = actions.GetQueue("h").Dequeue(null);
                    var m3 = actions.GetQueue("h").Dequeue(null);

                    Assert.Equal(new byte[] { 1 }, m1.Data);
                    Assert.Equal(new byte[] { 2 }, m2.Data);
                    Assert.Equal(new byte[] { 3 }, m3.Data);

                    actions.Commit();
                });
            }
        }

        [Fact]
        public void WillNotGiveMessageToTwoClient()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();

                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("h");
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var queue = actions.GetQueue("h");
                    var bookmark = queue.Enqueue(new Message
                    {
                        Queue = "h",
                        Id = MessageId.GenerateRandom(),
                        Data = new byte[] { 1 },
                    });
                    queue.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);

                    bookmark = queue.Enqueue(new Message
                    {
                        Queue = "h",
                        Id = MessageId.GenerateRandom(),
                        Data = new byte[] { 2 },
                    });
                    queue.SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);

                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var m1 = actions.GetQueue("h").Dequeue(null);

                    qf.Global(queuesActions =>
                    {
                        var m2 = queuesActions.GetQueue("h").Dequeue(null);
                        
                        Assert.Equal(new byte[] { 2 }, m2.Data);

                        queuesActions.Commit();
                    });

                   Assert.Equal(new byte[] { 1 }, m1.Data);
                   actions.Commit();
                });
            }
        }

        [Fact]
        public void WillGiveNullWhenNoItemsAreInQueue()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();
               
                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("h");
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var message = actions.GetQueue("h").Dequeue(null);
                    Assert.Null(message);
                    actions.Commit();
                });
            }
        }
    }
}