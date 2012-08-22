using System;
using System.Net;
using System.Threading;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Xunit;

namespace Rhino.Queues.Tests.Protocol
{
    public class SendingFailure : WithDebugging
    {
        private bool failureReported;
        private bool wasSuccessful;
        private readonly Sender sender;
        private readonly ManualResetEvent wait = new ManualResetEvent(false);
        private bool revertCalled;

        public SendingFailure()
        {
            sender = new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Messages = new[]
                {
                    new Message
                    {
                        Data = new byte[] {1, 2, 4},
                        Id = MessageId.GenerateRandom(),
                        Queue = "ag",
                        SentAt = new DateTime(2004, 4, 4)
                    },
                },
                Failure = exception => failureReported = true,
                FailureToConnect = exception => failureReported = true,
                Success = () =>
                {
                    wasSuccessful = true;
                    return null;
                },
                Revert = bookmarks => revertCalled = true
            };
            sender.SendCompleted += () => wait.Set();
        }

        [Fact]
        public void CanHandleItWhenReceiverDoesNotExists()
        {
            sender.Send();
            wait.WaitOne();

            Assert.True(failureReported);
            Assert.False(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverConnectAndDisconnect()
        {
            new FakeReceiver { DisconnectAfterConnect = true }.Start();

            sender.Send();
            wait.WaitOne();

            Assert.True(failureReported);
            Assert.False(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverDisconnectDuringRecieve()
        {
            new FakeReceiver { DisconnectDuringMessageSend = true }.Start();

            sender.Send();
            wait.WaitOne();

            Assert.True(failureReported);
            Assert.False(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverDisconnectAfterRecieve()
        {
            new FakeReceiver { DisconnectAfterMessageSend = true }.Start();

            sender.Send();
            wait.WaitOne();

            Assert.True(failureReported);
            Assert.False(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverSendingBadResponse()
        {
            new FakeReceiver { SendBadResponse = true }.Start();

            sender.Send();
            wait.WaitOne();

            Assert.True(failureReported);
            Assert.False(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverDisconnectAfterSendingRecieved()
        {
            new FakeReceiver { DisconnectAfterSendingReciept = true }.Start();

            sender.Send();
            wait.WaitOne();

            // this is a scenario where we actually have 
            // a false positive, this is an edge case that
            // we tolerate, since a message is not actually 
            // lost, but merely undeliverable in the receiver 
            // queue.

            Assert.False(failureReported);
            Assert.True(wasSuccessful);
        }

        [Fact]
        public void CanHandleItWhenReceiverSendRevert()
        {
            new FakeReceiver { FailOnAcknowledgement = true }.Start();

            sender.Send();
            wait.WaitOne();

            // this is a case where we create compensation
            // for reported failure on the Receiver side

            Assert.False(failureReported);
            Assert.True(wasSuccessful);
            Assert.True(revertCalled);
        }
    }
}