using System;
using System.Diagnostics;
using System.Linq;
using log4net;

namespace Rhino.Queues.Monitoring
{
    public class PerformanceCategoryCreator
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(PerformanceCategoryCreator));

        public PerformanceCategoryCreator()
        {
            try
            {
                CreateOutboundCategory();
                CreateInboundCategory();
            }
            catch (UnauthorizedAccessException ex)
            {
                logger.Error("Not authorized to create performance counters. User must be an administrator to perform this action.", ex);
            }
        }

        private void CreateOutboundCategory()
        {
            if (PerformanceCounterCategory.Exists(OutboundPerfomanceCounters.CATEGORY))
            {
                logger.DebugFormat("Deleting existing performance counter category '{0}'.", OutboundPerfomanceCounters.CATEGORY);
                PerformanceCounterCategory.Delete(OutboundPerfomanceCounters.CATEGORY);
            }

            logger.DebugFormat("Creating performance counter category '{0}'.", OutboundPerfomanceCounters.CATEGORY);

            try
            {
                var counters = new CounterCreationDataCollection(OutboundPerfomanceCounters.SupportedCounters().ToArray());
                PerformanceCounterCategory.Create(
                    OutboundPerfomanceCounters.CATEGORY,
                    "Provides statistics for Rhino-Queues messages out-bound from the current machine.",
                    PerformanceCounterCategoryType.MultiInstance,
                    counters);
            }
            catch (Exception ex)
            {
                logger.Error("Creation of outbound counters failed.", ex);
            }
        }

        private void CreateInboundCategory()
        {
            if (PerformanceCounterCategory.Exists(InboundPerfomanceCounters.CATEGORY))
            {
                logger.DebugFormat("Deleting existing performance counter category '{0}'.", InboundPerfomanceCounters.CATEGORY);
                PerformanceCounterCategory.Delete(InboundPerfomanceCounters.CATEGORY);
            }

            logger.DebugFormat("Creating performance counter category '{0}'.", InboundPerfomanceCounters.CATEGORY);

            try
            {
                var counters = new CounterCreationDataCollection(InboundPerfomanceCounters.SupportedCounters().ToArray());
                PerformanceCounterCategory.Create(
                    InboundPerfomanceCounters.CATEGORY,
                    "Provides statistics for Rhino-Queues messages in-bound to queues on the current machine.",
                    PerformanceCounterCategoryType.MultiInstance,
                    counters);
            }
            catch (Exception ex)
            {
                logger.Error("Creation of inbound counters failed.", ex);
            }
        }
    }
}
