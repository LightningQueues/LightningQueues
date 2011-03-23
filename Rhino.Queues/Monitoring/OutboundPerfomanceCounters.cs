using System;
using System.Collections.Generic;
using System.Diagnostics;
using log4net;

namespace Rhino.Queues.Monitoring
{
    public class OutboundPerfomanceCounters : IOutboundPerfomanceCounters
    {
        public const string CATEGORY = "Rhino-Queues Outbound";
        public const string UNSENT_COUNTER_NAME = "Unsent Messages";

        private readonly ILog logger = LogManager.GetLogger(typeof(OutboundPerfomanceCounters));
        private readonly string instanceName;

        public static IEnumerable<CounterCreationData> SupportedCounters()
        {
            yield return new CounterCreationData
                {
                    CounterType = PerformanceCounterType.NumberOfItems32,
                    CounterName = UNSENT_COUNTER_NAME,
                    CounterHelp = "Indicates the number of messages that are awaiting delivery to a queue.  Enable logging on the local queue to get more detailed diagnostic information."
                };
        }

        public OutboundPerfomanceCounters(string instanceName)
        {
            this.instanceName = instanceName;
            unsentMessages = new PerformanceCounter(CATEGORY, UNSENT_COUNTER_NAME, instanceName, false);
        }

        private readonly PerformanceCounter unsentMessages;
        public int UnsentMessages
        {
            get { return (int)unsentMessages.RawValue; }
            set
            {
                logger.DebugFormat("Setting UnsentMessages for instance '{0}' to {1}", instanceName, value);
                unsentMessages.RawValue = value;
            }
        }
            
    }
}