using System;
using System.IO;
using System.Linq;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Storage;
using Xunit;

namespace LightningQueues.Tests.Storage
{
    public class CanUseQueue
    {
        public CanUseQueue()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);
        }

        [Fact(Skip="Not on mono")]
        public void CanCreateNewQueueFactory()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanRegisterReceivedMessageIds()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                var random = MessageId.GenerateRandom();
                qf.Global(actions => actions.MarkReceived(random));

                qf.Global(actions => actions.GetAlreadyReceivedMessageIds().Contains(random).ShouldBeTrue());
            }
        }


        [Fact(Skip="Not on mono")]
        public void CanDeleteOldEntries()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                var random = MessageId.GenerateRandom();
                qf.Global(actions =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        actions.MarkReceived(MessageId.GenerateRandom());
                    }
                    actions.MarkReceived(random);

                    for (int i = 0; i < 5; i++)
                    {
                        actions.MarkReceived(MessageId.GenerateRandom());
                    }
                });


                qf.Global(actions =>
                {
                    actions.DeleteOldestReceivedMessageIds(6, 10).ToArray();//consume & activate
                });

                qf.Global(actions =>
                {
                    var array = actions.GetAlreadyReceivedMessageIds().ToArray();
                    6.ShouldEqual(array.Length);
                    random.ShouldEqual(array[0]);
                });
            }
        }

        [Fact(Skip="Not on mono")]
        public void CallingDeleteOldEntriesIsSafeIfThereAreNotEnoughEntries()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                var random = MessageId.GenerateRandom();
                qf.Global(actions =>
                {
                    for (int i = 0; i < 5; i++)
                    {
                        actions.MarkReceived(MessageId.GenerateRandom());
                    }
                    actions.MarkReceived(random);
                });


                qf.Global(actions =>
                {
                    actions.DeleteOldestReceivedMessageIds(10, 10).ToArray();//consume & activate
                });

                qf.Global(actions =>
                {
                    var array = actions.GetAlreadyReceivedMessageIds().ToArray();
                    6.ShouldEqual(array.Length);
                    random.ShouldEqual(array[5]);
                });


            }
        }

        [Fact(Skip="Not on mono")]
        public void CanPutSingleMessageInQueue()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                qf.Global(actions => actions.CreateQueueIfDoesNotExists("h"));

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
                });

                qf.Global(actions =>
                {
                    actions.GetQueue("h").SetMessageStatus(bookmark, MessageStatus.ReadyToDeliver);
                });

                qf.Global(actions =>
                {
                    var message = actions.GetQueue("h").Dequeue(null);

                    new byte[] { 13, 12, 43, 5 }.ShouldEqual(message.Data);
                    identifier.ShouldEqual(message.Id.MessageIdentifier);
                    guid.ShouldEqual(message.Id.SourceInstanceId);
                    "h".ShouldEqual(message.Queue);
                    new DateTime(2004, 5, 5).ShouldEqual(message.SentAt);
                });
            }
        }

        [Fact(Skip="Not on mono")]
        public void WillGetMessagesBackInOrder()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                qf.Global(actions => actions.CreateQueueIfDoesNotExists("h"));

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
                });

                qf.Global(actions =>
                {
                    var m1 = actions.GetQueue("h").Dequeue(null);
                    var m2 = actions.GetQueue("h").Dequeue(null);
                    var m3 = actions.GetQueue("h").Dequeue(null);

                    new byte[] { 1 }.ShouldEqual(m1.Data);
                    new byte[] { 2 }.ShouldEqual(m2.Data);
                    new byte[] { 3 }.ShouldEqual(m3.Data);
                });
            }
        }

        [Fact(Skip="Not on mono")]
        public void WillNotGiveMessageToTwoClient()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                qf.Global(actions => actions.CreateQueueIfDoesNotExists("h"));

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
                });

                qf.Global(actions =>
                {
                    var m1 = actions.GetQueue("h").Dequeue(null);

                    qf.Global(queuesActions =>
                    {
                        var m2 = queuesActions.GetQueue("h").Dequeue(null);

                        new byte[] { 2 }.ShouldEqual(m2.Data);
                    });

                    new byte[] { 1 }.ShouldEqual(m1.Data);
                });
            }
        }

        [Fact(Skip="Not on mono")]
        public void WillGiveNullWhenNoItemsAreInQueue()
        {
            using (var qf = CreateQueueStorage())
            {
                qf.Initialize();

                qf.Global(actions => actions.CreateQueueIfDoesNotExists("h"));

                qf.Global(actions =>
                {
                    var message = actions.GetQueue("h").Dequeue(null);
                    message.ShouldBeNull();
                });
            }
        }

        private static QueueStorage CreateQueueStorage()
        {
            return new QueueStorage("test.esent", new QueueManagerConfiguration());
        }
    }
}
