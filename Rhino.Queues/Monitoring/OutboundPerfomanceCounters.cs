using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Rhino.Queues.Monitoring
{
    internal class OutboundPerfomanceCounters : IOutboundPerfomanceCounters
    {
        private const string CATEGORY = "Rhino-Queues Outbound";
        private const string UNSENT_COUNTER_NAME = "Unsent Messages";

        static OutboundPerfomanceCounters()
        {
            var nonExistingOutboundCounters = new CounterCreationDataCollection(
                SupportedCounters()
                .Where(c => !PerformanceCounterCategory.CounterExists(CATEGORY, c.CounterName))
                .ToArray());

            PerformanceCounterCategory.Create(
                CATEGORY,
                "Provides statistics for Rhino-Queues messages out-bound from the current machine.",
                PerformanceCounterCategoryType.MultiInstance,
                nonExistingOutboundCounters);
        }

        private static IEnumerable<CounterCreationData> SupportedCounters()
        {
            yield return new CounterCreationData
                             {
                                 CounterType = PerformanceCounterType.NumberOfItems32,
                                 CounterName = UNSENT_COUNTER_NAME,
                                 CounterHelp = "Indicates the number of messages that are awaiting delivery to a queue.  Any significant number of unsent messages would be indicative of a communication problem with the remote queue or the remote queue being off line.  Enable logging on the local queue to get more detailed diagnostic information."
                             };
        }

        public OutboundPerfomanceCounters(string instanceName)
        {
            unsentMessages = new PerformanceCounter(CATEGORY, UNSENT_COUNTER_NAME, instanceName, false);
        }

        private readonly PerformanceCounter unsentMessages;
        public int UnsentMessages
        {
            get { return (int)unsentMessages.RawValue; }
            set { unsentMessages.RawValue = value; }
        }
            
    }
}