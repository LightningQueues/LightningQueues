using System.Net;
using System.Net.Sockets;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using Xunit;

namespace LightningQueues.Tests.Protocol
{
    public class SenderTester
    {
        [Fact(Skip = "Not on mono")]
        public void calls_connect_after_success()
        {
            var listener = new TcpListener(IPAddress.Any, 5500);
            listener.Start();
            listener.AcceptTcpClientAsync();
            bool connected = false;
            new Sender()
            {
                Connected = () => connected = true,
                Destination = new Endpoint("localhost", 5500),
                Messages = new[] { new Message { Data = System.Text.Encoding.UTF8.GetBytes("Hello") } },
            }.Send();

            Wait.Until(() => connected).ShouldBeTrue();
        }
    }
}