using System;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using LightningQueues.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Net.Tcp
{
    public class Sender : IDisposable
    {
        private readonly ISendingProtocol _protocol;
        private readonly IMessageStore _store;
        private readonly ILogger _logger;
        private readonly ISubject<OutgoingMessage> _outgoing;
        private IDisposable _sendingSubscription;

        public Sender(ISendingProtocol protocol, IMessageStore store, ILogger logger)
        {
            _protocol = protocol;
            _store = store;
            _logger = logger;
            _outgoing = new Subject<OutgoingMessage>();
        }

        public IObservable<Message> StartSending()
        {
            var outgoingByEndpoint = _store.PersistedOutgoingMessages()
                .Merge(_outgoing)
                .Buffer(TimeSpan.FromMilliseconds(100), TaskPoolScheduler.Default)
                .Where(x => x.Count > 0)
                .SelectMany(buffered =>
                {
                    _logger.Debug($"Buffered {buffered.Count} messages");
                    return buffered.GroupBy(x => x.Destination)
                    .Select(x => new OutgoingMessageBatch(x.Key, x));
                });
            var successfullySent = (from outgoing in outgoingByEndpoint
                                   let client = new TcpClient()
                                   from message in Observable.FromAsync(() => client.ConnectAsync(outgoing.Destination.Host, outgoing.Destination.Port))
                                                       .Timeout(TimeSpan.FromSeconds(5))
                                                       .Retry(5)
                                                       //CustomRetry here
                                                       .Select(x => client)
                                                       .Using(x =>
                                                       {
                                                           _logger.Debug($"Client connected to server at: {outgoing.Destination}");
                                                           outgoing.Stream = x.GetStream();
                                                           _logger.Debug($"Sending {outgoing.Messages.Count} through protocol");
                                                           return _protocol.SendStream(Observable.Return(outgoing));
                                                       })
                                   select message).Publish().RefCount();
            //todo something better than subscribe internally
            _sendingSubscription = successfullySent.Subscribe(x => { });
            return successfullySent;
        }

        public void Send(OutgoingMessage message)
        {
            _outgoing.OnNext(message);
        }

        public void Dispose()
        {
            if (_sendingSubscription != null)
            {
                _sendingSubscription.Dispose();
                _sendingSubscription = null;
            }
        }
    }
}