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

        public RecordingLogger() : this(x => { })
        {
        }

        public RecordingLogger(Action<string> outputHook)
        {
            _outputHook = outputHook;
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

        public void DebugFormat(string message, object arg1, object arg2)
        {
            Debug(string.Format(message, arg1, arg2));
        }

        public void DebugFormat(string message, object arg1)
        {
            Debug(string.Format(message, arg1));
        }

        public void Error(string message, Exception exception)
        {
            _outputHook(message + exception);
            _error.Add(message);
        }
    }
}