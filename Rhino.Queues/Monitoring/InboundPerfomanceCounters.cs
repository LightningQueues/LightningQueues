using System;
using System.Collections.Generic;
using System.Diagnostics;
using log4net;

namespace Rhino.Queues.Monitoring
{
    public class InboundPerfomanceCounters : IInboundPerfomanceCounters
    {
        private readonly string instanceName;
        public const string CATEGORY = "Rhino-Queues Inbound";
        public const string ARRIVED_COUNTER_NAME = "Arrived Messages";

        private readonly ILog logger = LogManager.GetLogger(typeof(InboundPerfomanceCounters));

        public static IEnumerable<CounterCreationData> SupportedCounters()
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
            this.instanceName = instanceName;
            try
            {
                arrivedMessages = new PerformanceCounter(CATEGORY, ARRIVED_COUNTER_NAME, instanceName, false);
            }
            catch (InvalidOperationException ex)
            {
                throw new ApplicationException(
                    string.Format(PerformanceCategoryCreator.CANT_CREATE_COUNTER_MSG, CATEGORY, ARRIVED_COUNTER_NAME),
                    ex);
            }
        }

        private readonly PerformanceCounter arrivedMessages;
        public int ArrivedMessages
        {
            get { return (int)arrivedMessages.RawValue; }
            set
            {
                logger.DebugFormat("Setting UnsentMessages for instance '{0}' to {1}", instanceName, value);
                arrivedMessages.RawValue = value;
            }
        }
            
    }
}