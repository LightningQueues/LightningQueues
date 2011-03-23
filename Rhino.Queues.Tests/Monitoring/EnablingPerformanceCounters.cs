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
    class EnablingPerformanceCounters : WithDebugging
    {
        private const string TEST_QUEUE_1 = "testA.esent";

        private void DeletePerformanceCounters()
        {
            if (PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY))
                PerformanceCounterCategory.Delete(OutboundPerfomanceCounters.CATEGORY);

            if (PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY))
                PerformanceCounterCategory.Delete(InboundPerfomanceCounters.CATEGORY);
        }

        public void ResetQueues()
        {
            if (Directory.Exists(TEST_QUEUE_1))
                Directory.Delete(TEST_QUEUE_1, true);
        }

        [Fact]
        public void Enabling_performance_counters_with_create_flag_should_create_categories()
        {
            DeletePerformanceCounters();

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                queueManager.EnablePerformanceCounters(true);
            }

            Assert.True(PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY));
            Assert.True(PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY));
        }

        [Fact]
        public void Enabling_performance_counters_without_create_flag_should_not_create_categories()
        {
            DeletePerformanceCounters();
            ResetQueues();  //Needed to ensure that PerformanceMonitor.SyncWithCurrentQueueState does nothing.
                            //Otherwise errors occur as a result of missing counters.

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                queueManager.EnablePerformanceCounters(false);
            }

            Assert.False(PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY));
            Assert.False(PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY));
        }

        [Fact]
        public void Enabling_performance_counters_with_create_flag_delete_and_recreate_existing_categories()
        {
            DeletePerformanceCounters();

            var preExistingCounters = new CounterCreationDataCollection(new[]
                  {
                      new CounterCreationData("DeleteMe", "",
                                              PerformanceCounterType.NumberOfItems32)
                  });

            PerformanceCounterCategory.Create(OutboundPerfomanceCounters.CATEGORY, "",
                PerformanceCounterCategoryType.MultiInstance, preExistingCounters);

            PerformanceCounterCategory.Create(InboundPerfomanceCounters.CATEGORY,"",
                PerformanceCounterCategoryType.MultiInstance, preExistingCounters);

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
            {
                queueManager.EnablePerformanceCounters(true);
            }

            Assert.False(PerformanceCounterCategory.CounterExists("DeleteMe", OutboundPerfomanceCounters.CATEGORY));
            Assert.False(PerformanceCounterCategory.CounterExists("DeleteMe", InboundPerfomanceCounters.CATEGORY));
        }

        [Fact]
        public void Enabling_performance_counters_should_syncronize_counters_with_current_queue_state()
        {
            ResetQueues();

            PerformanceCounter unsentQueueCounter;
            PerformanceCounter unsentSubQueueCounter;
            PerformanceCounter arrivedQueueCounter;
            PerformanceCounter arrivedSubQueueCounter;

            using (var queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1))
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
                queueManager.EnablePerformanceCounters(true);

                unsentQueueCounter = new PerformanceCounter(OutboundPerfomanceCounters.CATEGORY,
                                                            OutboundPerfomanceCounters.UNSENT_COUNTER_NAME, 
                                                            "localhost:23999/A");
                unsentSubQueueCounter = new PerformanceCounter(OutboundPerfomanceCounters.CATEGORY,
                                                               OutboundPerfomanceCounters.UNSENT_COUNTER_NAME, 
                                                               "localhost:23999/A/b");

                arrivedQueueCounter = new PerformanceCounter(InboundPerfomanceCounters.CATEGORY,
                                                             InboundPerfomanceCounters.ARRIVED_COUNTER_NAME, 
                                                             "127.0.0.1:23456/Z");

                arrivedSubQueueCounter = new PerformanceCounter(InboundPerfomanceCounters.CATEGORY,
                                                                InboundPerfomanceCounters.ARRIVED_COUNTER_NAME, 
                                                                "127.0.0.1:23456/Z/y");
            }

            Assert.Equal(1, unsentQueueCounter.RawValue);
            Assert.Equal(1, unsentSubQueueCounter.RawValue);
            Assert.Equal(1, arrivedQueueCounter.RawValue);
            Assert.Equal(1, arrivedSubQueueCounter.RawValue);
        }
    }
}
