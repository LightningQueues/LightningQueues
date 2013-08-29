using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using FubuCore.Logging;
using FubuTestingSupport;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;
using Rhino.Mocks;

namespace LightningQueues.Tests.Protocol
{
    [TestFixture]
    public class RecieverFailure
    {
        private readonly IPEndPoint _endpointToListenTo = new IPEndPoint(IPAddress.Loopback, 23456);
        private RecordingLogger _logger;

        [SetUp]
        public void Setup()
        {
            _logger = new RecordingLogger();
        }

        [Test]
        public void CanHandleClientConnectAndDisconnect()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                }

                Wait.Until(() => (from e in _logger.InfoMessages.OfType<StringMessage>()
                            where e.Message.StartsWith("Could not process Reading Length")
                            select e).Any()).ShouldBeTrue();
            }
        }

        [Test]
        public void CanHandleClientSendingThreeBytesAndDisconnecting()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
                }

                Wait.Until(() => (from e in _logger.InfoMessages.OfType<StringMessage>()
                            where e.Message.StartsWith("Could not process Reading Length")
                            select e).Any()).ShouldBeTrue();
            }
        }

        [Test]
        public void CanHandleClientSendingNegativeNumberForLength()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    client.GetStream().Write(BitConverter.GetBytes(-2), 0, 4);
                }

                Wait.Until(() => (from e in _logger.InfoMessages.OfType<StringMessage>()
                            where e.Message.Contains("Got invalid length -2")
                            select e).Any()).ShouldBeTrue();
            }
        }

        [Test]
        public void CanHandleClientSendingBadLengthOfData()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(BitConverter.GetBytes(5), 0, 4);
                }

                Wait.Until(() => (from e in _logger.InfoMessages.OfType<StringMessage>()
                            where e.Message.Contains("Could not process Reading Message")
                            select e).Any()).ShouldBeTrue();
            }
        }

        [Test]
        public void CanHandleClientSendingUnseriliazableData()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(Guid.NewGuid().ToByteArray(), 0, 16);
                }

                Wait.Until(() => (from e in _logger.InfoMessages.OfType<StringMessage>()
                            where e.Message.Contains("Unable to deserialize messages")
                            select e).Any()).ShouldBeTrue();
            }
        }

        [Test]
        public void WillLetSenderKnowThatMessagesWereNotProcessed()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages =>
            {
                throw new InvalidOperationException();
            }, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.ProcessingFailureBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.ProcessingFailureBuffer.ShouldEqual(buffer);
                }
            }
        }

        [Test]
        public void WillLetSenderKnowThatMessagesWereSentToInvalidQueue()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages =>
            {
                throw new QueueDoesNotExistsException();
            }, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.ProcessingFailureBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.QueueDoesNoExiststBuffer.ShouldEqual(buffer);
                }
            }
        }

        [Test]
        public void WillSendConfirmationForClient()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RecievedBuffer.ShouldEqual(buffer);
                }
            }
        }

        [Test]
        public void WillCallAbortAcceptanceIfSenderDoesNotConfirm()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RecievedBuffer.ShouldEqual(buffer);
                }
            }

            acceptance.AssertWasCalled(x => x.Abort());
        }

        [Test]
        public void WillCallAbortAcceptanceIfSenderSendNonConfirmation()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RecievedBuffer.ShouldEqual(buffer);

                    var bytes = Encoding.Unicode.GetBytes("Unknowledged");
                    stream.Write(bytes, 0, bytes.Length);
                }
            }

            acceptance.AssertWasCalled(x => x.Abort());
        }

        [Test]
        public void WillCallCommitAcceptanceIfSenderSendConfirmation()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RecievedBuffer.ShouldEqual(buffer);

                    stream.Write(ProtocolConstants.AcknowledgedBuffer, 0, ProtocolConstants.AcknowledgedBuffer.Length);
                }
            }

            acceptance.AssertWasCalled(x => x.Commit());
        }

        [Test]
        public void WillTellSenderIfCommitFailed()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Stub(x => x.Commit()).Throw(new InvalidOperationException());

            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RecievedBuffer.ShouldEqual(buffer);

                    stream.Write(ProtocolConstants.AcknowledgedBuffer, 0, ProtocolConstants.AcknowledgedBuffer.Length);

                    buffer = new byte[ProtocolConstants.RevertBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    ProtocolConstants.RevertBuffer.ShouldEqual(buffer);
                }
            }
        }
    }
}