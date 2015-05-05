using System;
//using NLog;

namespace LightningQueues.Logging
{
    public class NLogLogger : ILogger
    {
        //private readonly Logger _logger;

        public void Debug(string message)
        {
            //_logger.Debug(message, args);
        }

        public void Debug(Func<string> message)
        {
            //_logger.Debug(new LogMessageGenerator(message));
        }

        public void Info(string message, params object[] args)
        {
            //_logger.Info(message, args);
        }

        public void Info(string message, Exception exception, params object[] args)
        {
            //_logger.InfoException(message.ToFormat(args), exception);
        }

        public void Error(string message, Exception exception)
        {
            //_logger.ErrorException(message, exception);
        }

        public void Debug<TMessage>(TMessage message)
        {
        }

        public void Info(string message)
        {
        }

        public void Info<TMessage>(TMessage message)
        {
        }
    }
}