using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Transactions;
using FubuTestingSupport;
using Xunit;

namespace LightningQueues.Tests
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

        [Fact(Skip="Not on mono")]
        public void CanPurgeLargeSetsOfOldData()
        {
            queueManager = ObjectMother.QueueManager();
            queueManager.Configuration.OldestMessageInOutgoingHistory = TimeSpan.Zero;
            queueManager.Start();

            // Seed the queue with historical messages to be purged
            QueueMessagesThreaded(1000);
            //Parallel.For(0, 1000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i => SendMessages());

            queueManager.WaitForAllMessagesToBeSent();

            // Try to purge while still sending new messages.
            var waitHandle = new ManualResetEvent(false);
            ThreadPool.QueueUserWorkItem(_ =>
            {
                queueManager.PurgeOldData();
                Console.WriteLine("Finished purging data");
                waitHandle.Set();
            });
            QueueMessagesThreaded(10000);
            //var purgeTask = Task.Factory.StartNew(() =>
            //{
            //    queueManager.PurgeOldData();
            //    Console.WriteLine("Finished purging data");
            //});
            //Parallel.For(0, 10000, new ParallelOptions { MaxDegreeOfParallelism = 8 }, i => SendMessages());

            waitHandle.WaitOne();
            //purgeTask.Wait();
            queueManager.WaitForAllMessagesToBeSent();

            queueManager.PurgeOldData();

            queueManager.Configuration.NumberOfMessagesToKeepInOutgoingHistory.ShouldEqual(
                queueManager.GetAllSentMessages().Length);
        }

        private void QueueMessagesThreaded(int iterations)
        {
            const int threadCount = 8;
            int iterationsPerThread = iterations / threadCount;
            var threads = new List<Thread>();
            for (int i = 0; i < threadCount; i++)
            {
                var thread = new Thread(() =>
                {
                    for (int j = 0; j < iterationsPerThread; j++)
                    {
                        SendMessages();
                    }
                });
                thread.Start();
                threads.Add(thread);
            }

            threads.ForEach(x => x.Join());
        }

        private void SendMessages()
        {
            using (var scope = new TransactionScope())
            {
                for (int j = 0; j < 100; j++)
                {
                    queueManager.Send(new Uri("lq.tcp://" + queueManager.Endpoint),
                        new MessagePayload { Data = new byte[0] });
                }
                scope.Complete();
            }
        }
    }
}