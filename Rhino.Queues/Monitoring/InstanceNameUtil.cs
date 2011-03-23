using System;
using System.Net;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Monitoring
{
    internal static class InstanceNameUtil
    {
        public static string InboundInstanceName(this IQueueManager queueManager, Message message)
        {
            return queueManager.InboundInstanceName(message.Queue, message.SubQueue);
        }

        public static string InboundInstanceName(this IQueueManager queueManager, string queue, string subQueue)
        {
            return string.Format("{0}:{1}/{2}/{3}",
                                 queueManager.Endpoint.Address, queueManager.Endpoint.Port, queue, subQueue)
                .TrimEnd('/');
        }

        public static string OutboundInstanceName(this Endpoint endpoint, Message message)
        {
            return string.Format("{0}:{1}/{2}/{3}",
                                 endpoint.Host, endpoint.Port, message.Queue, message.SubQueue)
                .TrimEnd('/');
        }
    }
}
