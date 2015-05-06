using Xunit;
using Should;
using System;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using LightningQueues.Net.Tcp;
using LightningQueues.Net.Protocol.V1;
using LightningQueues.Storage;

namespace LightningQueues.Tests.Net.Tcp
{
    public class ProtocolTests
    {
        readonly SendingProtocol _sender;
        readonly Receiver _receiver;
        readonly IPEndPoint _endpoint;
        readonly RecordingLogger _logger;

        public ProtocolTests()
        {
            var port = PortFinder.FindPort(); //to make it possible to run in parallel
            _endpoint = new IPEndPoint(IPAddress.Loopback, port);
            _sender = new SendingProtocol();
            _logger = new RecordingLogger();
            var protocol = new ReceivingProtocol(new NoPersistenceMessageRepository(), _logger);
            _receiver = new Receiver(_endpoint, protocol);
        }

        [Fact]
        public void stops_listening_on_dispose_of_subscription()
        {
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
                try
                {
                    var listenerShouldThrow = new TcpListener(_endpoint);
                    listenerShouldThrow.Start();
                    true.ShouldBeFalse();
                }
                catch (Exception)
                {
                }
            }
            var listener = new TcpListener(_endpoint);
            listener.Start();
            listener.Stop();
        }

        [Fact]
        public void multiple_subscriptions_are_allowed()
        {
            using (_receiver.StartReceiving().Subscribe(x => { }))
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
        }

        [Fact]
        public void subscribe_unsubscribe_and_subscribe_again()
        {
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
            using (_receiver.StartReceiving().Subscribe(x => { }))
            {
            }
        }

        [Fact]
        public void can_handle_connect_then_disconnect()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using (var client = new TcpClient())
            {
                client.Connect(_endpoint);
            }
        }

        [Fact]
        public void can_handle_sending_three_bytes_then_disconnect()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using (var client = new TcpClient())
            {
                client.Connect(_endpoint);
                client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
            }
        }

        [Fact]
        public void accepts_concurrently_connected_clients()
        {
            using (_receiver.StartReceiving().Subscribe(x => true.ShouldBeFalse()))
            using(var client1 = new TcpClient())
            using(var client2 = new TcpClient())
            {
                client1.Connect(_endpoint);
                client2.Connect(_endpoint);
                client2.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
                client1.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
            }
        }
    }
}
