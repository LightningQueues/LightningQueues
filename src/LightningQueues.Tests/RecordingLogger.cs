using System;
using System.Collections.Generic;
using FubuCore;
using LightningQueues.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;

namespace LightningQueues.Tests
{
    public class RecordingLogger : ILogger
    {
        private readonly IList<string> _debug = new List<string>();
        private readonly IList<string> _error = new List<string>();
        private readonly IList<string> _info = new List<string>();
        private readonly IList<Message> _queuedForReceive = new List<Message>();
        private readonly IList<QueuedForSend> _queuedForSend = new List<QueuedForSend>();
        private readonly IList<Message> _received = new List<Message>();
        private readonly IList<SendFailure> _sendFailures = new List<SendFailure>();
        private readonly IList<SentMessages> _sent = new List<SentMessages>();

        public IEnumerable<string> InfoMessages
        {
            get { return _info; }
        }

        public IEnumerable<string> DebugMessages
        {
            get { return _debug; }
        }

        public IEnumerable<Message> MessagesQueuedForReceive
        {
            get { return _queuedForReceive; }
        }

        public IEnumerable<Message> ReceivedMessages
        {
            get { return _received; }
        }

        public IEnumerable<SentMessages> SentMessages
        {
            get { return _sent; }
        }

        public IEnumerable<SendFailure> SendFailures
        {
            get { return _sendFailures; }
        }

        public IEnumerable<QueuedForSend> MessagesQueuedForSend
        {
            get { return _queuedForSend; }
        }

        public void Debug(string message, params object[] args)
        {
            _debug.Add(message.ToFormat(args));
        }

        public void Debug(Func<string> message)
        {
            _debug.Add(message());
        }

        public void FailedToSend(Endpoint destination, string reason, Exception exception = null)
        {
            _sendFailures.Add(new SendFailure(destination, reason, exception));
        }

        public void QueuedForReceive(Message message)
        {
            _queuedForReceive.Add(message);
        }

        public void QueuedForSend(Message message, Endpoint destination)
        {
            _queuedForSend.Add(new QueuedForSend(message, destination));
        }

        public void MessageReceived(Message message)
        {
            _received.Add(message);
        }

        public void MessagesSent(IList<Message> messages, Endpoint destination)
        {
            _sent.Add(new SentMessages(destination, messages));
        }

        public void Info(string message, params object[] args)
        {
            _info.Add(message.ToFormat(args));
        }

        public void Info(string message, Exception exception, params object[] args)
        {
            _info.Add(message.ToFormat(args) + exception);
        }

        public void Error(string message, Exception exception)
        {
            _error.Add(message + exception);
        }
    }

    public class SentMessages
    {
        public SentMessages(Endpoint destination, IList<Message> messages)
        {
            Destination = destination;
            Messages = messages;
        }

        public Endpoint Destination { get; set; }
        public IList<Message> Messages { get; set; }
    }

    public class QueuedForSend
    {
        public QueuedForSend(Message message, Endpoint destination)
        {
            Message = message;
            Destination = destination;
        }

        public Endpoint Destination { get; set; }
        public Message Message { get; set; }
    }

    public class SendFailure
    {
        public SendFailure(Endpoint destination, string reason, Exception exception)
        {
            Destination = destination;
            Reason = reason;
            Exception = exception;
        }

        public Endpoint Destination { get; set; }
        public string Reason { get; set; }
        public Exception Exception { get; set; }
    }
}