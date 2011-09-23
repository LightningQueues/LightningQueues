using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Rhino.Mocks;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class AutoSelectPort : WithDebugging, IDisposable
    {
        private const int INITIAL_PORT = 23456;
        private readonly IPEndPoint endPoint = new IPEndPoint(IPAddress.Loopback, INITIAL_PORT);
        private readonly QueueManager queueManager;

        public AutoSelectPort()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            queueManager = new QueueManager(endPoint, "test.esent");
        }
        
        [Fact]
        public void ShouldUseInitialPortWhenItsNotInUse()
        {
            queueManager.EnableEndpointPortAutoSelection();
            queueManager.Start();

            Assert.Equal(INITIAL_PORT, queueManager.Endpoint.Port);
        }

        [Fact]
        public void ShouldThrowWhenInitialPortInUseAndAutoSelectDisabled()
        {
            var endPoint = new IPEndPoint(IPAddress.Loopback, INITIAL_PORT);

            var listener = new TcpListener(endPoint);
            listener.Start();
            try
            {
                Assert.Throws<SocketException>(() => queueManager.Start());
            }
            finally
            {
                listener.Stop();
            }
        }

        [Fact]
        public void ShouldUseAlternatePortWhenInitialPortInUseAndAutoSelectEnabled()
        {
            var listener = new TcpListener(endPoint);
            listener.Start();
            try
            {
                queueManager.EnableEndpointPortAutoSelection();
                queueManager.Start();

                Assert.NotEqual(INITIAL_PORT, queueManager.Endpoint.Port);
            }
            finally
            {
                listener.Stop();
            }
        }

        public void Dispose()
        {
            queueManager.Dispose();
        }
    }
}