using System;
using System.Threading;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests.Protocol
{
    [TestFixture]
    public class SendingFailure 
    {
        private bool failureReported;
        private bool wasSuccessful;
        private Sender sender;
        private ManualResetEvent wait;
        private bool revertCalled;

        [SetUp]
        public void Setup()
        {
            failureReported = wasSuccessful = revertCalled = false;
            wait = new ManualResetEvent(false);
            sender = new Sender(ObjectMother.Logger())
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

        [TearDown]
        public void Teardown()
        {
            sender.Dispose();
        }

        [Test]
        public void CanHandleItWhenReceiverDoesNotExists()
        {
            sender.Send();
            wait.WaitOne();

            failureReported.ShouldBeTrue();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverConnectAndDisconnect()
        {
            StartReceiver(x => x.DisconnectAfterConnect = true);

            failureReported.ShouldBeTrue();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverDisconnectDuringRecieve()
        {
            StartReceiver(x => x.DisconnectDuringMessageSend = true);

            failureReported.ShouldBeTrue();
            wasSuccessful.ShouldBeFalse();
        }

        private void StartReceiver(Action<FakeReceiver> receiverAction)
        {
            var receiver = new FakeReceiver();
            receiverAction(receiver);
            receiver.Start();

            sender.Send();
            wait.WaitOne();
        }

        [Test]
        public void CanHandleItWhenReceiverDisconnectAfterRecieve()
        {
            StartReceiver(x => x.DisconnectAfterMessageSend = true);

            failureReported.ShouldBeTrue();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverSendingBadResponse()
        {
            StartReceiver(x => x.SendBadResponse = true);

            failureReported.ShouldBeTrue();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverDisconnectAfterSendingRecieved()
        {
            StartReceiver(x => x.DisconnectAfterSendingReciept = true);

            // this is a scenario where we actually have 
            // a false positive, this is an edge case that
            // we tolerate, since a message is not actually 
            // lost, but merely undeliverable in the receiver 
            // queue.

            failureReported.ShouldBeFalse();
            wasSuccessful.ShouldBeTrue();
        }

        [Test]
        public void CanHandleItWhenReceiverSendRevert()
        {
            StartReceiver(x => x.FailOnAcknowledgement = true);

            // this is a case where we create compensation
            // for reported failure on the Receiver side

            failureReported.ShouldBeFalse();
            wasSuccessful.ShouldBeTrue();
            revertCalled.ShouldBeTrue();
        }
    }
}