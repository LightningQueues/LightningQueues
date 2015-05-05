using System;

namespace LightningQueues.Logging
{
    public interface ILogger
    {
        void Debug(string message);
        void Debug<TMessage>(TMessage message);
        void Info(string message);
        void Info<TMessage>(TMessage message);
        void Error(string message, Exception exception);
    }
}