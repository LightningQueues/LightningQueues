using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using log4net;

namespace Rhino.Queues.Monitoring
{
    public class PerformanceMonitor : IDisposable
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(PerformanceMonitor));
        
        private readonly IQueueManager queueManager;
        public PerformanceMonitor(IQueueManager queueManager)
        {
            this.queueManager = queueManager;

            AssertCountersExist();
            AttachToEvents();
            SyncWithCurrentQueueState();
        }

        public void Dispose()
        {
            DetachFromEvents();
        }

        protected virtual void AssertCountersExist()
        {
            OutboundPerfomanceCounters.AssertCountersExist();
            InboundPerfomanceCounters.AssertCountersExist();
        }

        private void SyncWithCurrentQueueState()
        {
            logger.Debug("Begin synchronization of performance counters with queue state.");

            foreach (var queueName in queueManager.Queues)
            {
                var queueCount = queueManager.GetNumberOfMessages(queueName);

                foreach (var subQueue in queueManager.GetSubqueues(queueName))
                {
                    //HACK: There is currently no direct way to get just a count of messages in a subqueue
                    var count = queueManager.GetAllMessages(queueName,subQueue).Length;
                    queueCount -= count;
                    var counters = GetInboundCounters(queueManager.InboundInstanceName(queueName, subQueue));
                    lock (counters)
                    {
                        counters.ArrivedMessages = count;
                    }
                }

                var queueCounters = GetInboundCounters(queueManager.InboundInstanceName(queueName, string.Empty));
                lock (queueCounters)
                {
                    queueCounters.ArrivedMessages = queueCount;
                }
            }

            foreach (var counter in from m in queueManager.GetMessagesCurrentlySending()
                                      group m by m.Endpoint.OutboundInstanceName(m) into c
                                      select new {InstanceName = c.Key, Count = c.Count()})
            {
                var outboundCounter = GetOutboundCounters(counter.InstanceName);
                lock (outboundCounter)
                {
                    outboundCounter.UnsentMessages = counter.Count;
                }
            }
            logger.Debug("Synchronization complete.");
        }

        private void AttachToEvents()
        {
            queueManager.MessageQueuedForSend += OnMessageQueuedForSend;
            queueManager.MessageSent += OnMessageSent;
            queueManager.MessageQueuedForReceive += OnMessageQueuedForReceive;
            queueManager.MessageReceived += OnMessageReceived;
        }

        private void DetachFromEvents()
        {
            queueManager.MessageQueuedForSend -= OnMessageQueuedForSend;
            queueManager.MessageSent -= OnMessageSent;
            queueManager.MessageQueuedForReceive -= OnMessageQueuedForReceive;
            queueManager.MessageReceived -= OnMessageReceived;
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

        private IOutboundPerfomanceCounters GetOutboundCounters(MessageEventArgs e)
        {
            var instanceName = e.Endpoint.OutboundInstanceName(e.Message);
            return GetOutboundCounters(instanceName);
        }

        private IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
        {
            return GetPerformanceCounterProviderForCurrentTransaction().GetOutboundCounters(instanceName);
        }

        private IInboundPerfomanceCounters GetInboundCounters(MessageEventArgs e)
        {
            var instanceName = queueManager.InboundInstanceName(e.Message);
            return GetInboundCounters(instanceName);
        }

        private IInboundPerfomanceCounters GetInboundCounters(string instanceName)
        {
            return GetPerformanceCounterProviderForCurrentTransaction().GetInboundCounters(instanceName);
        }

        private readonly Dictionary<Transaction,IPerformanceCountersProvider> transactionalProviders = new Dictionary<Transaction, IPerformanceCountersProvider>();
        private IPerformanceCountersProvider GetPerformanceCounterProviderForCurrentTransaction()
        {
            var transaction = Transaction.Current;

            if(transaction == null) 
                return ImmediatelyRecordingProvider;

            IPerformanceCountersProvider provider;
            if (transactionalProviders.TryGetValue(transaction, out provider))
                return provider;

            lock (transactionalProviders)
            {
                if (transactionalProviders.TryGetValue(transaction, out provider))
                    return provider;

                provider = new TransactionalPerformanceCountersProvider(transaction, ImmediatelyRecordingProvider);

                transactionalProviders.Add(transaction, provider);
                transaction.TransactionCompleted += (s, e) =>
                    {
                        lock (transactionalProviders)
                        {
                            transactionalProviders.Remove(e.Transaction);
                        }
                    };

                return provider;
            }
        }

        private readonly IPerformanceCountersProvider immediatelyRecordingProvider = new ImmediatelyRecordingCountersProvider();
        protected virtual IPerformanceCountersProvider ImmediatelyRecordingProvider
        {
            get { return immediatelyRecordingProvider; }
        }
    }
}
