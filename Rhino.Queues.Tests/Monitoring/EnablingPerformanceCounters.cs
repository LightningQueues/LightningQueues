using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Transactions;
using Rhino.Queues.Monitoring;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests.Monitoring
{
    public class EnablingPerformanceCounters : WithDebugging
    {
        private const string TEST_QUEUE_1 = "testA.esent";

        public void Setup()
        {
            if (Directory.Exists(TEST_QUEUE_1))
                Directory.Delete(TEST_QUEUE_1, true);

            new PerformanceCategoryCreator();
        }

        [Fact]
        public void Enabling_performance_counters_without_existing_categories_throws_meaningful_error()
        {
            PerformanceCounterCategoryCreation.DeletePerformanceCounters();

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                Assert.Throws<ApplicationException>( queueManager.EnablePerformanceCounters );
            }
        }

        [Fact]
        public void Enabling_performance_counters_should_syncronize_counters_with_current_queue_state()
        {
            Setup();

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                EnqueueMessages(queueManager);
                queueManager.EnablePerformanceCounters();
            }

            AssertAllCountersHaveCorrectValues();
        }

        [Fact]
        public void After_enabling_performance_counters_changes_to_queue_state_should_be_reflected_in_counters()
        {
            Setup();

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                queueManager.EnablePerformanceCounters();
                EnqueueMessages(queueManager);
            }

            AssertAllCountersHaveCorrectValues();
        }

        private void EnqueueMessages(QueueManager queueManager)
        {
            queueManager.CreateQueues("Z");
            
            using (var tx = new TransactionScope())
            {
                queueManager.EnqueueDirectlyTo("Z", "", new MessagePayload {Data = new byte[] {1, 2, 3}});
                queueManager.EnqueueDirectlyTo("Z", "y", new MessagePayload {Data = new byte[] {1, 2, 3}});
                queueManager.Send(new Uri("rhino.queues://localhost:23999/A"),
                                  new MessagePayload {Data = new byte[] {1, 2, 3}});
                queueManager.Send(new Uri("rhino.queues://localhost:23999/A/b"),
                                  new MessagePayload {Data = new byte[] {1, 2, 3}});

                tx.Complete();
            }
        }

        private void AssertAllCountersHaveCorrectValues()
        {
            var unsentQueueCounter = new PerformanceCounter(OutboundPerfomanceCounters.CATEGORY,
                                                            OutboundPerfomanceCounters.UNSENT_COUNTER_NAME,
                                                            "localhost:23999/A");
            var unsentSubQueueCounter = new PerformanceCounter(OutboundPerfomanceCounters.CATEGORY,
                                                               OutboundPerfomanceCounters.UNSENT_COUNTER_NAME,
                                                               "localhost:23999/A/b");

            var arrivedQueueCounter = new PerformanceCounter(InboundPerfomanceCounters.CATEGORY,
                                                             InboundPerfomanceCounters.ARRIVED_COUNTER_NAME,
                                                             "127.0.0.1:23456/Z");

            var arrivedSubQueueCounter = new PerformanceCounter(InboundPerfomanceCounters.CATEGORY,
                                                                InboundPerfomanceCounters.ARRIVED_COUNTER_NAME,
                                                                "127.0.0.1:23456/Z/y");

            Assert.Equal(1, unsentQueueCounter.RawValue);
            Assert.Equal(1, unsentSubQueueCounter.RawValue);
            Assert.Equal(1, arrivedQueueCounter.RawValue);
            Assert.Equal(1, arrivedSubQueueCounter.RawValue);
        }
    }
}
