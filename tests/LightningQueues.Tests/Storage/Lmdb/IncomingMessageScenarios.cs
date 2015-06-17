using System;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Xunit;
using static LightningQueues.Tests.ObjectMother;

namespace LightningQueues.Tests.Storage.Lmdb
{
    [Collection("SharedTestDirectory")]
    public class IncomingMessageScenarios : IDisposable
    {
        private readonly LmdbMessageStore _store;

        public IncomingMessageScenarios(SharedTestDirectory testDirectory)
        {
            var queuePath = testDirectory.CreateNewDirectoryForTest();
            _store = new LmdbMessageStore(queuePath);
        }

        [Fact]
        public void storing_message_for_queue_that_doesnt_exist()
        {
            var message = NewIncomingMessage();
            Assert.Throws<QueueDoesNotExistException>(() => _store.StoreMessages(message));
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}