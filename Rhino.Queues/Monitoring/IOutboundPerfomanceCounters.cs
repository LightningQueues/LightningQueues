using System;

namespace Rhino.Queues.Monitoring
{
    public interface IOutboundPerfomanceCounters
    {
        int UnsentMessages { get; set; }
    }
}