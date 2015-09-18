using System;

namespace LightningQueues.Logging
{
    public class NulloLogger : ILogger
    {
        public void Debug(string message)
        {
        }

        public void DebugFormat(string message, params object[] args)
        {
        }

        public void Debug(Func<string> message)
        {
        }

        public void Info(string message, params object[] args)
        {
        }

        public void Info(string message, Exception exception, params object[] args)
        {
        }

        public void Error(string message, Exception exception)
        {
        }

        public void ErrorFormat(string message, Exception ex, params object[] args)
        {
        }

        public void Debug<TMessage>(TMessage message)
        {
        }

        public void Info(string message)
        {
        }

        public void InfoFormat(string message, params object[] args)
        {
        }

        public void Info<TMessage>(TMessage message)
        {
        }
    }
}