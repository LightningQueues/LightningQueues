using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Repository.Hierarchy;
using Rhino.Mocks;
using Rhino.Queues.Exceptions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Xunit;

namespace Rhino.Queues.Tests.Protocol
{
    public class RecieverFailure : WithDebugging, IDisposable
    {
        private readonly ManualResetEvent wait = new ManualResetEvent(false);
        private readonly IPEndPoint endpointToListenTo = new IPEndPoint(IPAddress.Loopback, 23456);
        private readonly MemoryAppender appender = new MemoryAppender();
        private readonly Logger logger;

        public RecieverFailure()
        {
            logger = ((Logger)(LogManager.GetLogger(typeof(Receiver)).Logger));
            logger.AddAppender(appender);
        }

        [Fact]
        public void CanHandleClientConnectAndDisconnect()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages => null))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                }

                wait.WaitOne();

                var warn = (from e in appender.GetEvents()
                            where e.Level == Level.Warn &&
                                  e.RenderedMessage.StartsWith("Unable to read length data from")
                            select e).FirstOrDefault();

                Assert.NotNull(warn);
            }
        }

        [Fact]
        public void CanHandleClientSendingThreeBytesAndDisconnecting()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages => null))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    client.GetStream().Write(new byte[] { 1, 4, 6 }, 0, 3);
                }

                wait.WaitOne();

                var warn = (from e in appender.GetEvents()
                            where e.Level == Level.Warn &&
                                  e.RenderedMessage.StartsWith("Unable to read length data from")
                            select e).FirstOrDefault();

                Assert.NotNull(warn);
            }
        }

        [Fact]
        public void CanHandleClientSendingNegativeNumberForLength()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages => null))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    client.GetStream().Write(BitConverter.GetBytes(-2), 0, 4);
                }

                wait.WaitOne();

                var warn = (from e in appender.GetEvents()
                            where e.Level == Level.Warn &&
                                  e.RenderedMessage.StartsWith("Got invalid length -2")
                            select e).FirstOrDefault();

                Assert.NotNull(warn);
            }
        }

        [Fact]
        public void CanHandleClientSendingBadLengthOfData()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages => null))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(BitConverter.GetBytes(5), 0, 4);
                }

                wait.WaitOne();

                var warn = (from e in appender.GetEvents()
                            where e.Level == Level.Warn &&
                                  e.RenderedMessage.StartsWith("Unable to read message data")
                            select e).FirstOrDefault();

                Assert.NotNull(warn);
            }
        }

        [Fact]
        public void CanHandleClientSendingUnseriliazableData()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages => null))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    stream.Write(BitConverter.GetBytes(16), 0, 4);
                    stream.Write(Guid.NewGuid().ToByteArray(), 0, 16);
                }

                wait.WaitOne();

                var warn = (from e in appender.GetEvents()
                            where e.Level == Level.Warn &&
                                  e.RenderedMessage.StartsWith("Failed to deserialize messages from")
                            select e).FirstOrDefault();

                Assert.NotNull(warn);
            }
        }

        [Fact]
        public void WillLetSenderKnowThatMessagesWereNotProcessed()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages =>
            {
                throw new InvalidOperationException(); 
            }))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.ProcessingFailureBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.ProcessingFailure, Encoding.Unicode.GetString(buffer));
                }

                wait.WaitOne();
            }
        }

        [Fact]
        public void WillLetSenderKnowThatMessagesWereSentToInvalidQueue()
        {
            using (var reciever = new Receiver(endpointToListenTo, messages =>
            {
                throw new QueueDoesNotExistsException();
            }))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.ProcessingFailureBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.QueueDoesNotExists, Encoding.Unicode.GetString(buffer));
                }

                wait.WaitOne();
            }
        }

        [Fact]
        public void WillSendConfirmationForClient()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(endpointToListenTo, messages => acceptance))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Recieved, Encoding.Unicode.GetString(buffer));
                }

                wait.WaitOne();
            }
        }

        [Fact]
        public void WillCallAbortAcceptanceIfSenderDoesNotConfirm()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(endpointToListenTo, messages => acceptance))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Recieved, Encoding.Unicode.GetString(buffer));
                }

                wait.WaitOne();
            }

            acceptance.AssertWasCalled(x => x.Abort());
        }

        [Fact]
        public void WillCallAbortAcceptanceIfSenderSendNonConfirmation()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(endpointToListenTo, messages => acceptance))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Recieved, Encoding.Unicode.GetString(buffer));

                    var bytes = Encoding.Unicode.GetBytes("Unknowledged");
                    stream.Write(bytes, 0, bytes.Length);
                }

                wait.WaitOne();
            }

            acceptance.AssertWasCalled(x => x.Abort());
        }

        [Fact]
        public void WillCallCommitAcceptanceIfSenderSendConfirmation()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            using (var reciever = new Receiver(endpointToListenTo, messages => acceptance))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Recieved, Encoding.Unicode.GetString(buffer));

                    stream.Write(ProtocolConstants.AcknowledgedBuffer, 0, ProtocolConstants.AcknowledgedBuffer.Length);
                }

                wait.WaitOne();
            }

            acceptance.AssertWasCalled(x => x.Commit());
        }

        [Fact]
        public void WillTellSenderIfCommitFailed()
        {
            var acceptance = MockRepository.GenerateStub<IMessageAcceptance>();
            acceptance.Stub(x => x.Commit()).Throw(new InvalidOperationException());

            using (var reciever = new Receiver(endpointToListenTo, messages => acceptance))
            {
                reciever.CompletedRecievingMessages += () => wait.Set();
                reciever.Start();

                using (var client = new TcpClient())
                {
                    client.Connect(endpointToListenTo);
                    var stream = client.GetStream();
                    var serialize = new Message[0].Serialize();
                    stream.Write(BitConverter.GetBytes(serialize.Length), 0, 4);
                    stream.Write(serialize, 0, serialize.Length);

                    var buffer = new byte[ProtocolConstants.RecievedBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Recieved, Encoding.Unicode.GetString(buffer));

                    stream.Write(ProtocolConstants.AcknowledgedBuffer, 0, ProtocolConstants.AcknowledgedBuffer.Length);

                    buffer = new byte[ProtocolConstants.RevertBuffer.Length];
                    stream.Read(buffer, 0, buffer.Length);

                    Assert.Equal(ProtocolConstants.Revert, Encoding.Unicode.GetString(buffer));
                }

                wait.WaitOne();
            }
        }

        public void Dispose()
        {
            logger.RemoveAppender(appender);
        }
    }
}