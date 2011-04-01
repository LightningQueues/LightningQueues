using System;
using System.Collections.Generic;
using System.Transactions;

namespace Rhino.Queues.Monitoring
{
    public class TransactionalPerformanceCountersProvider : IPerformanceCountersProvider
    {
        private readonly IPerformanceCountersProvider providerToCommitTo;
        private readonly Dictionary<string, TransactionalOutboundPerformanceCounters> outboundCounters = new Dictionary<string, TransactionalOutboundPerformanceCounters>();
        private readonly Dictionary<string, TransactionalInboundPerformanceCounters> inboundCounters = new Dictionary<string, TransactionalInboundPerformanceCounters>();

        public TransactionalPerformanceCountersProvider(Transaction transaction, IPerformanceCountersProvider providerToCommitTo)
        {
            this.providerToCommitTo = providerToCommitTo;
            transaction.TransactionCompleted += HandleTransactionCompleted;
        }

        void HandleTransactionCompleted(object sender, TransactionEventArgs e)
        {
            if (e.Transaction.TransactionInformation.Status != TransactionStatus.Committed) return;

            foreach (var counters in outboundCounters)
                counters.Value.CommittTo(providerToCommitTo.GetOutboundCounters(counters.Key));
            foreach (var counters in inboundCounters)
                counters.Value.CommittTo(providerToCommitTo.GetInboundCounters(counters.Key));
        }

        public IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
        {
            TransactionalOutboundPerformanceCounters counters;
            if (outboundCounters.TryGetValue(instanceName, out counters))
                return counters;

            lock (outboundCounters)
            {
                if (outboundCounters.TryGetValue(instanceName, out counters))
                    return counters;

                counters = new TransactionalOutboundPerformanceCounters();
                outboundCounters.Add(instanceName, counters);

                return counters;
            }
        }

        public IInboundPerfomanceCounters GetInboundCounters(string instanceName)
        {
            TransactionalInboundPerformanceCounters counters;
            if (inboundCounters.TryGetValue(instanceName, out counters))
                return counters;

            lock (inboundCounters)
            {
                if (inboundCounters.TryGetValue(instanceName, out counters))
                    return counters;

                counters = new TransactionalInboundPerformanceCounters();
                inboundCounters.Add(instanceName, counters);

                return counters;
            }
        }

        private class TransactionalOutboundPerformanceCounters : IOutboundPerfomanceCounters
        {
            public int UnsentMessages { get; set; }

            public void CommittTo(IOutboundPerfomanceCounters counters)
            {
                counters.UnsentMessages += UnsentMessages;
            }
        }

        private class TransactionalInboundPerformanceCounters : IInboundPerfomanceCounters
        {
            public int ArrivedMessages { get; set; }

            public void CommittTo(IInboundPerfomanceCounters counters)
            {
                counters.ArrivedMessages += ArrivedMessages;
            }
        }
    }
}