using System;
using System.Diagnostics;
using Rhino.Queues.Monitoring;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests.Monitoring
{
    public class PerformanceCounterCategoryCreation : WithDebugging
    {
        public static void DeletePerformanceCounters()
        {
            if (PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY))
                PerformanceCounterCategory.Delete(OutboundPerfomanceCounters.CATEGORY);

            if (PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY))
                PerformanceCounterCategory.Delete(InboundPerfomanceCounters.CATEGORY);
        }


        [Fact]
        public void Create_performance_counters_categories()
        {
            DeletePerformanceCounters();

            PerformanceCategoryCreator.CreateCategories();

            Assert.True(PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY));
            Assert.True(PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY));
        }

        [Fact]
        public void Recreate_existing_categories()
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

            PerformanceCategoryCreator.CreateCategories();

            Assert.False(PerformanceCounterCategory.CounterExists("DeleteMe", OutboundPerfomanceCounters.CATEGORY));
            Assert.False(PerformanceCounterCategory.CounterExists("DeleteMe", InboundPerfomanceCounters.CATEGORY));
        }
    }
}
