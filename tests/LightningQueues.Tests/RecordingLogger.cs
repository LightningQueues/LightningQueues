using System;
using System.Collections.Generic;
using LightningQueues.Logging;

namespace LightningQueues.Tests
{
    public class RecordingLogger : ILogger
    {
        readonly IList<string> _debug = new List<string>();
        readonly IList<string> _error = new List<string>();
        readonly IList<string> _info = new List<string>();

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
            _debug.Add(message);
        }

        public void Debug<TMessage>(TMessage message)
        {
            Debug(message.ToString());
        }

        public void Info(string message)
        {
            _info.Add(message);
        }

        public void Info<TMessage>(TMessage message)
        {
            Info(message.ToString());
        }

        public void Error(string message, Exception exception)
        {
            _error.Add(message);
        }
    }
}