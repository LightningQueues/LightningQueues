using System;
using System.Collections.Generic;
using FubuCore.Logging;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    public class CaptureAllLogListener : ILogListener
    {
        private List<string> _logStatements = new List<string>();

        public bool ListensFor(Type type)
        {
            return type == typeof(string);
        }

        public void DebugMessage(object message)
        {
            throw new NotImplementedException();
        }

        public void InfoMessage(object message)
        {
            throw new NotImplementedException();
        }

        public void Debug(string message)
        {
            _logStatements.Add(message);
        }

        public void Info(string message)
        {
            _logStatements.Add(message);
        }

        public void Error(string message, Exception ex)
        {
            _logStatements.Add(message);
        }

        public void Error(object correlationId, string message, Exception ex)
        {
            _logStatements.Add(message);
        }

        public IEnumerable<string> AllLogs {get { return _logStatements; }}
        public bool IsDebugEnabled { get; private set; }
        public bool IsInfoEnabled { get; private set; }
    }
}