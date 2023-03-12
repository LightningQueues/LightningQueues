using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using LightningQueues.Net;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Net.Tcp;
using LightningQueues.Storage;
using Microsoft.Extensions.Logging;

namespace LightningQueues;

public class QueueConfiguration
{
    #region NoLogging
    private class NoLoggingLogger : ILogger
    {
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return false;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }
    }
    #endregion //NoLogging
    
    private IStreamSecurity _sendingSecurity;
    private IStreamSecurity _receivingSecurity;
    private IMessageStore _store;
    private IPEndPoint _endpoint;
    private IReceivingProtocol _receivingProtocol;
    private ISendingProtocol _sendingProtocol;
    private ILogger _logger;
    private TimeSpan _timeoutBatchAfter;

    public QueueConfiguration()
    {
        _logger = new NoLoggingLogger();
        _sendingSecurity = new NoSecurity();
        _receivingSecurity = new NoSecurity();
        _timeoutBatchAfter = TimeSpan.FromSeconds(5);
    }

    public QueueConfiguration StoreMessagesWith(IMessageStore store)
    {
        _store = store;
        return this;
    }

    public QueueConfiguration TimeoutNetworkBatchAfter(TimeSpan timeSpan)
    {
        _timeoutBatchAfter = timeSpan;
        return this;
    }

    public QueueConfiguration ReceiveMessagesAt(IPEndPoint endpoint)
    {
        _endpoint = endpoint;
        return this;
    }

    public QueueConfiguration CommunicateWithProtocol(IReceivingProtocol receivingProtocol, ISendingProtocol sendingProtocol)
    {
        _receivingProtocol = receivingProtocol;
        _sendingProtocol = sendingProtocol;
        return this;
    }

    public QueueConfiguration AutomaticEndpoint()
    {
        return ReceiveMessagesAt(new IPEndPoint(IPAddress.Loopback, PortFinder.FindPort()));
    }

    public QueueConfiguration LogWith(ILogger logger)
    {
        _logger = logger;
        return this;
    }

    public QueueConfiguration SecureTransportWith(Func<Uri, Stream, Task<Stream>> receivingSecurity, Func<Uri, Stream, Task<Stream>> sendingSecurity)
    {
        _receivingSecurity = new TlsStreamSecurity(sendingSecurity);
        _sendingSecurity = new TlsStreamSecurity(receivingSecurity);
        return this;
    }

    public Queue BuildQueue()
    {
        if(_store == null)
            throw new ArgumentNullException(nameof(_store), "Storage has not been configured. Are you missing a call to StoreMessagesWith?");

        if(_endpoint == null)
            throw new ArgumentNullException(nameof(_endpoint), "Endpoint has not been configured. Are you missing a call to ReceiveMessageAt?");

        InitializeDefaults();


        var receiver = new Receiver(_endpoint, _receivingProtocol, _logger);
        var sender = new Sender(_sendingProtocol, _logger, _timeoutBatchAfter);
        var queue = new Queue(receiver, sender, _store, _logger);
        return queue;
    }

    private void InitializeDefaults()
    {
        _sendingProtocol ??= new SendingProtocol(_store, _sendingSecurity, _logger);
        _receivingProtocol ??= new ReceivingProtocol(_store, _receivingSecurity, 
            new Uri($"lq.tcp://{_endpoint}"), _logger);
    }
}