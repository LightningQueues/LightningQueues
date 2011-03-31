using System;

namespace Rhino.Queues.Monitoring
{
    public interface IPerformanceCounterProvider
    {
        IOutboundPerfomanceCounters GetOutboundCounters(string instanceName);
        IInboundPerfomanceCounters GetInboundCounters(string instanceName);
    }
}