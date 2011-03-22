using System.Collections.Generic;

namespace Rhino.Queues.Monitoring
{
    public class PerformanceMonitor
    {
        private readonly IQueueManager queueManager;

        public PerformanceMonitor(IQueueManager queueManager)
        {
            this.queueManager = queueManager;
            AttachToEvents();
        }

        private void AttachToEvents()
        {
            queueManager.MessageQueuedForSend += OnMessageQueuedForSend;
            queueManager.MessageSent += OnMessageSent;
            queueManager.MessageQueuedForReceive += OnMessageQueuedForReceive;
            queueManager.MessageReceived += OnMessageReceived;
        }

        private void OnMessageQueuedForSend(object source, MessageEventArgs e)
        {
            var counters = GetOutboundCounters(e);
            lock(counters)
            {
                counters.UnsentMessages++;
            }
        }

        private void OnMessageSent(object source, MessageEventArgs e)
        {
            var counters = GetOutboundCounters(e);
            lock (counters)
            {
                counters.UnsentMessages--;
            }
        }

        private void OnMessageQueuedForReceive(object source, MessageEventArgs e)
        {
            var counters = GetInboundCounters(e);
            lock (counters)
            {
                counters.ArrivedMessages++;
            }
        }

        private void OnMessageReceived(object source, MessageEventArgs e)
        {
            var counters = GetInboundCounters(e);
            lock (counters)
            {
                counters.ArrivedMessages--;
            }
        }

        private readonly Dictionary<string, IOutboundPerfomanceCounters> outboundCounters = new Dictionary<string, IOutboundPerfomanceCounters>();
        private IOutboundPerfomanceCounters GetOutboundCounters(MessageEventArgs e)
        {
            var instanceName = GetInstanceName(e);

            IOutboundPerfomanceCounters counter;
            if (!outboundCounters.TryGetValue(instanceName, out counter))
            {
                lock (outboundCounters)
                {
                    if (!outboundCounters.TryGetValue(instanceName, out counter))
                    {
                        counter = GetOutboundCounters(instanceName);
                        outboundCounters.Add(instanceName, counter);
                    }
                }
            }
            return counter;
        }

        protected virtual IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
        {
            return new OutboundPerfomanceCounters(instanceName);
        }

        private readonly Dictionary<string, IInboundPerfomanceCounters> inboundCounters = new Dictionary<string, IInboundPerfomanceCounters>();
        private IInboundPerfomanceCounters GetInboundCounters(MessageEventArgs e)
        {
            var instanceName = GetInstanceName(e);

            IInboundPerfomanceCounters counter;
            if (!inboundCounters.TryGetValue(instanceName, out counter))
            {
                lock (outboundCounters)
                {
                    if (!inboundCounters.TryGetValue(instanceName, out counter))
                    {
                        counter = GetInboundCounters(instanceName);
                        inboundCounters.Add(instanceName, counter);
                    }
                }
            }
            return counter;
        }

        protected virtual IInboundPerfomanceCounters GetInboundCounters(string instanceName)
        {
            return new InboundPerfomanceCounters(instanceName);
        }

        private string GetInstanceName(MessageEventArgs e)
        {
            return string.Format("{0}:{1}/{2}/{3}", 
                                 e.Endpoint.Host, e.Endpoint.Port, e.Message.Queue, e.Message.SubQueue)
                .TrimEnd('/');
        }
    }
}
