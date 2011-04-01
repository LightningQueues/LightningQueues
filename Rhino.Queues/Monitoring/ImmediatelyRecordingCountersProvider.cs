using System.Collections.Generic;

namespace Rhino.Queues.Monitoring
{
    internal class ImmediatelyRecordingCountersProvider : IPerformanceCountersProvider
    {
        private readonly Dictionary<string, IOutboundPerfomanceCounters> outboundCounters = new Dictionary<string, IOutboundPerfomanceCounters>();
        public IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
        {
            IOutboundPerfomanceCounters counter;
            if (!outboundCounters.TryGetValue(instanceName, out counter))
            {
                lock (outboundCounters)
                {
                    if (!outboundCounters.TryGetValue(instanceName, out counter))
                    {
                        counter = new OutboundPerfomanceCounters(instanceName); 
                        outboundCounters.Add(instanceName, counter);
                    }
                }
            }
            return counter;
        }
        
        private readonly Dictionary<string, IInboundPerfomanceCounters> inboundCounters = new Dictionary<string, IInboundPerfomanceCounters>();
        public IInboundPerfomanceCounters GetInboundCounters(string instanceName)
        {
            IInboundPerfomanceCounters counter;
            if (!inboundCounters.TryGetValue(instanceName, out counter))
            {
                lock (outboundCounters)
                {
                    if (!inboundCounters.TryGetValue(instanceName, out counter))
                    {
                        counter = new InboundPerfomanceCounters(instanceName);
                        inboundCounters.Add(instanceName, counter);
                    }
                }
            }
            return counter;
        }
    }
}