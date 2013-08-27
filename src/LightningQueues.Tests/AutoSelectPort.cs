using System.IO;
using System.Net;
using System.Net.Sockets;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class AutoSelectPort 
    {
        private const int InitialPort = 23456;
        private readonly IPEndPoint _endPoint = new IPEndPoint(IPAddress.Loopback, InitialPort);
        private QueueManager _queueManager;

        [SetUp]
        public void Setup()
        {
            _queueManager = ObjectMother.QueueManager();
        }
        
        [Test]
        public void ShouldUseInitialPortWhenItsNotInUse()
        {
            _queueManager.EnableEndpointPortAutoSelection();
            _queueManager.Start();

            InitialPort.ShouldEqual(_queueManager.Endpoint.Port);
        }

        [Test]
        public void ShouldThrowWhenInitialPortInUseAndAutoSelectDisabled()
        {
            var endPoint = new IPEndPoint(IPAddress.Loopback, InitialPort);

            var listener = new TcpListener(endPoint);
            listener.Start();
            try
            {
                Assert.Throws<SocketException>(() => _queueManager.Start());
            }
            finally
            {
                listener.Stop();
            }
        }

        [Test]
        public void ShouldUseAlternatePortWhenInitialPortInUseAndAutoSelectEnabled()
        {
            var listener = new TcpListener(_endPoint);
            listener.Start();
            try
            {
                _queueManager.EnableEndpointPortAutoSelection();
                _queueManager.Start();

                InitialPort.ShouldNotEqual(_queueManager.Endpoint.Port);
            }
            finally
            {
                listener.Stop();
            }
        }

        [TearDown]
        public void TearDown()
        {
            _queueManager.Dispose();
        }
    }
}