using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using FubuTestingSupport;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol;
using Xunit;
using Rhino.Mocks;

namespace LightningQueues.Tests.Protocol
{
    public class RecieverFailure
    {
        private readonly IPEndPoint _endpointToListenTo = new IPEndPoint(IPAddress.Loopback, 23456);
        private RecordingLogger _logger;

        public RecieverFailure()
        {
            _logger = new RecordingLogger();
            ReceivingProtocol.Logger = _logger;
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleClientConnectAndDisconnect()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.StartsWith("Could not process Reading Length"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleClientSendingThreeBytesAndDisconnecting()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.StartsWith("Could not process Reading Length"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleClientSendingNegativeNumberForLength()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    client.GetStream().Write(BitConverter.GetBytes(-2), 0, 4);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.Contains("Got invalid length -2"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleClientSendingBadLengthOfData()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(BitConverter.GetBytes(5), 0, 4);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.Contains("Could not process Reading Message"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleClientSendingUnseriliazableData()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null))
            {
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(Guid.NewGuid().ToByteArray(), 0, 16);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.Contains("Unable to deserialize messages"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleSlowClients()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages => null, _logger))
            {
                reciever.Timeout = TimeSpan.FromMilliseconds(500);
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(_endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    Thread.Sleep(1000);
                    stream.Write(Guid.NewGuid().ToByteArray(), 0, 16);
                }

                Wait.Until(() => _logger.InfoMessages
                    .Any(x => x.Contains("TimeoutException"))).ShouldBeTrue();
            }
        }

        [Fact(Skip="Not on mono")]
        public void WillLetSenderKnowThatMessagesWereNotProcessed()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages =>
            {
                throw new InvalidOperationException();
            }))
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

        [Fact(Skip="Not on mono")]
        public void WillLetSenderKnowThatMessagesWereSentToInvalidQueue()
        {
            using (var reciever = new Receiver(_endpointToListenTo, messages =>
            {
                throw new QueueDoesNotExistsException();
            }))
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

        [Fact(Skip="Not on mono")]
        public void WillSendConfirmationForClient()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance))
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

        [Fact(Skip="Not on mono")]
        public void WillCallAbortAcceptanceIfSenderDoesNotConfirm()
        {
            var abortCalled = false;
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Expect(x => x.Abort()).WhenCalled(x => abortCalled = true);

            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance))
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

            Wait.Until(() => abortCalled);

            acceptance.VerifyAllExpectations();
        }

        [Fact(Skip="Not on mono")]
        public void WillCallAbortAcceptanceIfSenderSendNonConfirmation()
        {
            var abortCalled = false;
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Expect(x => x.Abort()).WhenCalled(x => abortCalled = true);
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance))
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

            Wait.Until(() => abortCalled);

            acceptance.VerifyAllExpectations();
        }

        [Fact(Skip="Not on mono")]
        public void WillCallCommitAcceptanceIfSenderSendConfirmation()
        {
            var commitCalled = false;
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Expect(x => x.Commit()).WhenCalled(x => commitCalled = true);
            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance))
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

            Wait.Until(() => commitCalled);

            acceptance.VerifyAllExpectations();
        }

        [Fact(Skip="Not on mono")]
        public void WillTellSenderIfCommitFailed()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Stub(x => x.Commit()).Throw(new InvalidOperationException());

            using (var reciever = new Receiver(_endpointToListenTo, messages => acceptance))
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