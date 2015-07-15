using System;
using System.Threading.Tasks;
using LightningQueues.Storage.LMDB;
using Xunit;

namespace LightningQueues.Tests.Storage.Lmdb
{
    [Collection("SharedTestDirectory")]
    public class OutgoingMessageScenarios : IDisposable
    {
        private readonly string _queuePath;
        private LmdbMessageStore _store;

        public OutgoingMessageScenarios(SharedTestDirectory testDirectory)
        {
            _queuePath = testDirectory.CreateNewDirectoryForTest();
            _store = new LmdbMessageStore(_queuePath);
            _store.CreateQueue("outgoing");
        }

        [Fact]
        public async Task happy_path_messages_sent()
        {
            var destination = new Uri("lq.tcp://localhost:5050");
            var message = ObjectMother.NewMessage<OutgoingMessage>("test");
            message.Destination = destination;
            var message2 = ObjectMother.NewMessage<OutgoingMessage>("test");
            message2.Destination = destination;
            message2.DeliverBy = DateTime.Now.AddSeconds(5);
            message2.MaxAttempts = 3;
            message2.Headers.Add("header", "headervalue");
            var tx = _store.BeginTransaction();
            _store.StoreOutgoing(tx, message);
            _store.StoreOutgoing(tx, message2);
            tx.Commit();
            var asyncTx = await _store.Async.BeginTransaction();
            await _store.Async.SuccessfullySent(asyncTx, message);
            await asyncTx.Commit();

            var result = await _store.PersistedOutgoingMessages().FirstAsyncWithTimeout();
            result.Id.ShouldEqual(message2.Id);
            result.Queue.ShouldEqual(message2.Queue);
            result.Data.ShouldEqual(message2.Data);
            result.SentAt.ShouldEqual(message2.SentAt);
            result.DeliverBy.ShouldEqual(message2.DeliverBy);
            result.MaxAttempts.ShouldEqual(message2.MaxAttempts);
            result.Headers["header"].ShouldEqual("headervalue");
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}