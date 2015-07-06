using System;
using System.Linq;
using System.Text;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Xunit;
using static LightningQueues.Tests.ObjectMother;

namespace LightningQueues.Tests.Storage.Lmdb
{
    [Collection("SharedTestDirectory")]
    public class IncomingMessageScenarios : IDisposable
    {
        private readonly string _queuePath;
        private LmdbMessageStore _store;

        public IncomingMessageScenarios(SharedTestDirectory testDirectory)
        {
            _queuePath = testDirectory.CreateNewDirectoryForTest();
            _store = new LmdbMessageStore(_queuePath);
        }

        [Fact]
        public void happy_path_success()
        {
            var message = NewIncomingMessage();
            _store.CreateQueue(message.Queue);
            _store.StoreMessages(message).Commit();
            using (var tx = _store.Environment.BeginTransaction())
            {
                using (var db = tx.OpenDatabase(message.Queue))
                {
                    var data = tx.Get(db, Encoding.UTF8.GetBytes($"{message.Id}"));
                    var headers = tx.Get(db, Encoding.UTF8.GetBytes($"{message.Id}/headers")).ToDictionary();
                    Encoding.UTF8.GetString(data).ShouldEqual("hello");
                    headers.First().Value.ShouldEqual("myvalue");
                }
            }
        }

        [Fact]
        public void storing_message_for_queue_that_doesnt_exist()
        {
            var message = NewIncomingMessage();
            Assert.Throws<QueueDoesNotExistException>(() => _store.StoreMessages(message));
        }

        [Fact]
        public void crash_before_commit()
        {
            var message = NewIncomingMessage();
            _store.CreateQueue(message.Queue);
            _store.StoreMessages(message);
            _store.Dispose();
            //crash
            _store = new LmdbMessageStore(_queuePath);
            using (var tx = _store.Environment.BeginTransaction())
            {
                using (var db = tx.OpenDatabase(message.Queue))
                {
                    var result = tx.Get(db, Encoding.UTF8.GetBytes($"id/{message.Id}"));
                    result.ShouldBeNull();
                }
            }
        }

        [Fact]
        public void rollback_messages_received()
        {
            var message = NewIncomingMessage();
            _store.CreateQueue(message.Queue);
            var transaction = _store.StoreMessages(message);
            transaction.Rollback();
            using (var tx = _store.Environment.BeginTransaction())
            {
                using (var db = tx.OpenDatabase(message.Queue))
                {
                    var result = tx.Get(db, Encoding.UTF8.GetBytes($"id/{message.Id}"));
                    result.ShouldBeNull();
                }
            }
        }

        [Fact]
        public void creating_multiple_stores()
        {
            _store.Dispose();
            _store = new LmdbMessageStore(_queuePath);
            _store.Dispose();
            _store = new LmdbMessageStore(_queuePath);
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}