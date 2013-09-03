using System;
using FubuCore.Logging;
using LightningQueues.Protocol;

namespace LightningQueues.Logging
{
    public class FailedToSend : LogRecord
    {
        public FailedToSend(Endpoint destination, string reason, Exception exception = null)
        {
            Destination = destination;
            Reason = reason;
            Exception = exception;
        }

        public Endpoint Destination { get; private set; }
        public string Reason { get; private set; }
        public Exception Exception { get; private set; }
    }
}