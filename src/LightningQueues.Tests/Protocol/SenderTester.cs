using System.Net;
using System.Net.Sockets;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests.Protocol
{
    [TestFixture]
    public class SenderTester
    {
        [Test]
        public void calls_connect_after_success()
        {
            var listener = new TcpListener(IPAddress.Any, 5500);
            listener.Start();
            listener.AcceptTcpClientAsync();
            bool connected = false;
            new Sender(new RecordingLogger())
            {
                Connected = () => connected = true,
                Destination = new Endpoint("localhost", 5500),
                Messages = new[] { new Message{Data = System.Text.Encoding.UTF8.GetBytes("Hello")} },
            }.Send();

            Wait.Until(() => connected).ShouldBeTrue();
        }
    }
}