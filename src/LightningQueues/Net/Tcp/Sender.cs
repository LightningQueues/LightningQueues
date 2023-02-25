using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using System.Threading;
using LightningQueues.Logging;
using LightningQueues.Net.Security;

namespace LightningQueues.Net.Tcp;

public class Sender : IDisposable
{
    private readonly ILogger _logger;
    private readonly ISendingProtocol _protocol;
    private readonly IStreamSecurity _security;
    private readonly ISubject<OutgoingMessageFailure> _failedToSend;
    private IObservable<OutgoingMessage> _outgoingStream;
    private IObservable<OutgoingMessage> _successfullySent;
    private IDisposable _sendingSubscription;

    public Sender(ILogger logger, ISendingProtocol protocol, IStreamSecurity security)
    {
        _logger = logger;
        _protocol = protocol;
        _security = security;
        _failedToSend = new Subject<OutgoingMessageFailure>();
    }

    public IObservable<OutgoingMessageFailure> FailedToSend() => _failedToSend;

    public async IAsyncEnumerable<OutgoingMessage> StartSending(IAsyncEnumerable<OutgoingMessage> outgoing,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var source = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, source.Token);
        await foreach (var batch in outgoing.Buffer(TimeSpan.FromMilliseconds(200), 50, linked.Token))
        {
            var batchByEndpoint = batch.GroupBy(x => x.Destination);
            foreach (var sendTo in batchByEndpoint)
            {
                var uri = sendTo.Key;
                var client = new TcpClient();
                if (Dns.GetHostName() == uri.Host)
                {
                    await client.ConnectAsync(IPAddress.Loopback, uri.Port, linked.Token);
                }
                else
                {
                    await client.ConnectAsync(uri.Host, uri.Port, linked.Token);
                }

                foreach (var msg in sendTo)
                {
                    yield return msg;
                }
            }
        }
    }

    public IObservable<OutgoingMessage> StartSending(IObservable<OutgoingMessage> outgoingStream)
    {
        _outgoingStream = outgoingStream;
        _successfullySent = SuccessfullySentMessages().Publish().RefCount();
        _sendingSubscription = _successfullySent.Subscribe(_ => { });
        return _successfullySent;
    }

    private IObservable<OutgoingMessage> SuccessfullySentMessages()
    {
        return ConnectedOutgoingMessageBatch()
            .Using(x => _protocol.Send(x).Timeout(TimeSpan.FromSeconds(5))
                .Catch<OutgoingMessage, Exception>(ex => HandleException<OutgoingMessage>(ex, x)));
    }

    private IObservable<OutgoingMessageBatch> ConnectedOutgoingMessageBatch()
    {
        return AllOutgoingMessagesBatchedByEndpoint()
            .SelectMany(batch =>
            {
                _logger.DebugFormat("Preparing to send {0} messages to {1}", batch.Messages.Count,
                    batch.Destination);
                return Observable.FromAsync(batch.ConnectAsync, TaskPoolScheduler.Default)
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Catch<Unit, Exception>(ex => HandleException<Unit>(ex, batch))
                    .Select(_ => batch);
            });
    }

    private IObservable<OutgoingMessageBatch> AllOutgoingMessagesBatchedByEndpoint()
    {
        return BufferedAllOutgoingMessages()
            .SelectMany(x =>
            {
                return x.GroupBy(grouped => grouped.Destination)
                    .Select(grouped => new OutgoingMessageBatch(grouped.Key, grouped, new TcpClient(), _security));
            });
    }

    private IObservable<IList<OutgoingMessage>> BufferedAllOutgoingMessages()
    {
        return AllOutgoingMessages().Buffer(TimeSpan.FromMilliseconds(200))
            .Where(x => x.Count > 0);
    }

    private IObservable<OutgoingMessage> AllOutgoingMessages()
    {
        return _outgoingStream;
    }

    private IObservable<T> HandleException<T>(Exception ex, OutgoingMessageBatch batch)
    {
        _logger.Error($"Got an error sending message to {batch.Destination}", ex);
        _failedToSend.OnNext(new OutgoingMessageFailure { Batch = batch, Exception = ex });
        return Observable.Empty<T>();
    }

    public void Dispose()
    {
        _logger.Info("Disposing Sender to ");
        if (_sendingSubscription == null) return;
        _sendingSubscription.Dispose();
        _sendingSubscription = null;
        GC.SuppressFinalize(this);
    }
}