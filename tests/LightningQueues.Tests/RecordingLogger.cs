using System;
using System.Collections.Generic;
using LightningQueues.Logging;

namespace LightningQueues.Tests
{
    public class RecordingLogger : ILogger
    {
        private readonly Action<string> _outputHook;
        readonly IList<string> _debug = new List<string>();
        readonly IList<string> _error = new List<string>();
        readonly IList<string> _info = new List<string>();

        public RecordingLogger() : this(x => { })
        {
        }

        public RecordingLogger(Action<string> outputHook)
        {
            _outputHook = outputHook;
        }

        public IEnumerable<string> InfoMessages
        {
            get { return _info; }
        }

        public IEnumerable<string> DebugMessages
        {
            get { return _debug; }
        }

        public IEnumerable<string> ErrorMessages
        {
            get { return _error; }
        }

        public void Debug(string message)
        {
            _outputHook(message);
            _debug.Add(message);
        }

        public void DebugFormat(string message, params object[] args)
        {
            Debug(string.Format(message, args));
        }


        public void Debug<TMessage>(TMessage message)
        {
            Debug(message.ToString());
        }

        public void Info(string message)
        {
            _outputHook(message);
            _info.Add(message);
        }

        public void InfoFormat(string message, params object[] args)
        {
            Info(string.Format(message, args));
        }

        public void Info<TMessage>(TMessage message)
        {
            Info(message.ToString());
        }

        public void Error(string message, Exception exception)
        {
            _outputHook(message + exception);
            _error.Add(message);
        }

        public void ErrorFormat(string message, Exception ex, params object[] args)
        {
            Error(string.Format(message, args), ex);
        }
    }
}