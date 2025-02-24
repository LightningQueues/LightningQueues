using System;
using System.Net;
using LightningDB;
using LightningQueues.Net;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Net.Security;
using LightningQueues.Net.Tcp;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
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
    private Func<IMessageStore> _store;
    private IPEndPoint _endpoint;
    private IReceivingProtocol _receivingProtocol;
    private ISendingProtocol _sendingProtocol;
    private IMessageSerializer _serializer;
    private ILogger _logger;
    private TimeSpan _timeoutBatchAfter;
    
    public IMessageSerializer Serializer => _serializer;

    public QueueConfiguration StoreMessagesWith(Func<IMessageStore> store)
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

    public QueueConfiguration SerializeWith(IMessageSerializer serializer)
    {
        _serializer = serializer;
        return this;
    }

    public QueueConfiguration LogWith(ILogger logger)
    {
        _logger = logger;
        return this;
    }

    public QueueConfiguration SecureTransportWith(IStreamSecurity sending, IStreamSecurity receiving)
    {
        _receivingSecurity = receiving;
        _sendingSecurity = sending;
        return this;
    }

    public QueueConfiguration WithDefaults(string path = null)
    {
        SerializeWith(new MessageSerializer());
        LogWith(new NoLoggingLogger());
        SecureTransportWith(new NoSecurity(), new NoSecurity());
        TimeoutNetworkBatchAfter(TimeSpan.FromSeconds(5));
        AutomaticEndpoint();
        if(path != null)
            this.StoreWithLmdb(path, new EnvironmentConfiguration { MaxDatabases = 5, MapSize = 1024 * 1024 * 100 });
        return this;
    }

    public Queue BuildQueue()
    {
        if(_store == null)
            throw new ArgumentNullException(nameof(_store), "Storage has not been configured. Are you missing a call to StoreMessagesWith?");

        if(_endpoint == null)
            throw new ArgumentNullException(nameof(_endpoint), "Endpoint has not been configured. Are you missing a call to ReceiveMessageAt?");

        var serializer = new MessageSerializer();
        var store = _store();
        _sendingProtocol ??= new SendingProtocol(store, _sendingSecurity, serializer, _logger);
        _receivingProtocol ??= new ReceivingProtocol(store, _receivingSecurity, serializer, new Uri($"lq.tcp://{_endpoint}"), _logger);


        var receiver = new Receiver(_endpoint, _receivingProtocol, _logger);
        var sender = new Sender(_sendingProtocol, _logger, _timeoutBatchAfter);
        var queue = new Queue(receiver, sender, store, _logger);
        return queue;
    }

    public Queue BuildAndStart(string queueName)
    {
        var queue = BuildQueue();
        queue.CreateQueue(queueName);
        queue.Start();
        return queue;
    }
}