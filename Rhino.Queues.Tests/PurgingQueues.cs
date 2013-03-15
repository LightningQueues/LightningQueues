using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class PurgingQueues
    {
        private const string EsentFileName = "test.esent";
        private QueueManager queueManager;

        public PurgingQueues()
        {
            if (Directory.Exists(EsentFileName))
                Directory.Delete(EsentFileName, true);
        }

        [Fact(Skip = "This is a slow load test")]
        public void CanPurgeLargeSetsOfOldData()
        {
            queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), EsentFileName);
            queueManager.Configuration.OldestMessageInOutgoingHistory = TimeSpan.Zero;
            queueManager.Start();

            // Seed the queue with historical messages to be purged
            Parallel.For(0, 1000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i => SendMessages());

            queueManager.WaitForAllMessagesToBeSent();

            // Try to purge while still sending new messages.
            var purgeTask = Task.Factory.StartNew(() =>
            {
                queueManager.PurgeOldData();
                Console.WriteLine("Finished purging data");
            });
            Parallel.For(0, 10000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i => SendMessages());

            purgeTask.Wait();
            queueManager.WaitForAllMessagesToBeSent();

            queueManager.PurgeOldData();

            Assert.Equal(queueManager.Configuration.NumberOfMessagesToKeepInOutgoingHistory,
                queueManager.GetAllSentMessages().Length);
        }

        private void SendMessages()
        {
            using (var scope = new TransactionScope())
            {
                for (int j = 0; j < 100; j++)
                {
                    queueManager.Send(new Uri("rhino.queues://" + queueManager.Endpoint),
                        new MessagePayload { Data = new byte[0] });
                }
                scope.Complete();
            }
        }
    }
}