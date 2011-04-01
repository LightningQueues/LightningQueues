using System;

namespace Rhino.Queues.Monitoring
{
    public interface IPerformanceCountersProvider
    {
        IOutboundPerfomanceCounters GetOutboundCounters(string instanceName);
        IInboundPerfomanceCounters GetInboundCounters(string instanceName);
    }
}