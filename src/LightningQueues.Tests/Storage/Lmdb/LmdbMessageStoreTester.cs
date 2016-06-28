using System;
using System.Linq;
using System.Reactive.Linq;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;

namespace LightningQueues.Tests.Storage.Lmdb
{
    [Collection("SharedTestDirectory")]
    public class LmdbMessageStoreTester : IDisposable
    {
        private readonly LmdbMessageStore _store;

        public LmdbMessageStoreTester(SharedTestDirectory testDirectory)
        {
            _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        }

        [Fact]
        public void getting_all_queues()
        {
            _store.CreateQueue("test");
            _store.CreateQueue("test2");
            _store.CreateQueue("test3");
            _store.GetAllQueues().SequenceEqual(new[] {"test", "test2", "test3"}).ShouldBeTrue();
        }

        [Fact]
        public void clear_all_history_with_empty_dataset_doesnt_throw()
        {
            _store.CreateQueue("test");
            _store.ClearAllStorage();
        }

        [Fact]
        public void clear_all_history_with_persistent_data()
        {
            _store.CreateQueue("test");
            var message = ObjectMother.NewMessage<Message>("test");
            var outgoingMessage = ObjectMother.NewMessage<OutgoingMessage>();
            outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
            outgoingMessage.SentAt = DateTime.Now;
            var tx = _store.BeginTransaction();
            _store.StoreOutgoing(tx, outgoingMessage);
            _store.StoreIncomingMessages(tx, message);
            tx.Commit();
            _store.PersistedMessages("test").ToEnumerable().Count().ShouldBe(1);
            _store.PersistedOutgoingMessages().ToEnumerable().Count().ShouldBe(1);
            _store.ClearAllStorage();
            _store.PersistedMessages("test").ToEnumerable().Count().ShouldBe(0);
            _store.PersistedOutgoingMessages().ToEnumerable().Count().ShouldBe(0);
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}