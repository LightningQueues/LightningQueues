using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Rhino.Queues.Monitoring
{
    internal class InboundPerfomanceCounters : IInboundPerfomanceCounters
    {
        private const string CATEGORY = "Rhino-Queues Inbound";
        private const string ARRIVED_COUNTER_NAME = "Arrived Messages";
            
        static InboundPerfomanceCounters()
        {
            var nonExistingInboundCounters = new CounterCreationDataCollection(
                SupportedCounters()
               .Where(c => !PerformanceCounterCategory.CounterExists(CATEGORY, c.CounterName))
               .ToArray());

            PerformanceCounterCategory.Create(
                CATEGORY,
                "Provides statistics for Rhino-Queues messages in-bound to queues on the current machine.",
                PerformanceCounterCategoryType.MultiInstance,
                nonExistingInboundCounters);
        }

        private static IEnumerable<CounterCreationData> SupportedCounters()
        {
            yield return new CounterCreationData
                             {
                                 CounterType = PerformanceCounterType.NumberOfItems32,
                                 CounterName = ARRIVED_COUNTER_NAME,
                                 CounterHelp = "Indicates the number of messages that have arrived in the queue but have not been received by the queue's client.  Enable logging on the queue to get more detailed diagnostic information."
                             };
        }

        public InboundPerfomanceCounters(string instanceName)
        {
            arrivedMessages = new PerformanceCounter(CATEGORY, ARRIVED_COUNTER_NAME, instanceName, false);
        }

        private readonly PerformanceCounter arrivedMessages;
        public int ArrivedMessages
        {
            get { return (int)arrivedMessages.RawValue; }
            set { arrivedMessages.RawValue = value; }
        }
            
    }
}