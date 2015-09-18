using System;

namespace LightningQueues.Logging
{
    public interface ILogger
    {
        void Debug(string message);
        void DebugFormat(string message, params object[] args);
        void Debug<TMessage>(TMessage message);
        void Info(string message);
        void InfoFormat(string message, params object[] args);
        void Info<TMessage>(TMessage message);
        void Error(string message, Exception exception);
        void ErrorFormat(string message, Exception ex, params object[] args);
    }
}