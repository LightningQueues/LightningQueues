using System.Linq;
using LightningQueues.Storage.LMDB;
using Xunit;

namespace LightningQueues.Tests.Storage.Lmdb
{
    [Collection("SharedTestDirectory")]
    public class LmdbMessageStoreTester
    {
        private LmdbMessageStore _store;

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
    }
}