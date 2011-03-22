using System;

namespace Rhino.Queues.Monitoring
{
    public interface IInboundPerfomanceCounters
    {
        int ArrivedMessages { get; set; }
    }
}