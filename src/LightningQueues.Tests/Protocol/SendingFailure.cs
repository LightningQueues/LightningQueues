using System;
using System.IO;
using System.Linq;
using FubuTestingSupport;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol;
using NUnit.Framework;

namespace LightningQueues.Tests.Protocol
{
    [TestFixture]
    public class SendingFailure 
    {
        private Exception error;
        private bool wasSuccessful;
        private Sender sender;
        private RecordingLogger _logger;

        [SetUp]
        public void Setup()
        {
            _logger = new RecordingLogger();
            wasSuccessful = false;
            error = null;
            sender = new Sender()
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
                Success = () =>
                {
                    wasSuccessful = true;
                },
            };
        }

        [Test]
        public void CanHandleItWhenReceiverDoesNotExists()
        {
            var task = sender.Send();
            var aggregateException = Assert.Throws<AggregateException>(task.Wait);
            aggregateException.InnerExceptions.OfType<FailedToConnectException>().Any().ShouldBeTrue();

            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverConnectAndDisconnect()
        {
            StartReceiver(x => x.DisconnectAfterConnect = true);

            error.ShouldBeOfType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverDisconnectDuringRecieve()
        {
            StartReceiver(x => x.DisconnectDuringMessageSend = true);

            error.ShouldBeOfType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        private void StartReceiver(Action<FakeReceiver> receiverAction)
        {
            var receiver = new FakeReceiver();
            receiverAction(receiver);
            receiver.Start();

            try
            {
                var task = sender.Send();
                task.Wait();
            }
            catch (AggregateException ex)
            {
                error = ex.InnerExceptions.First();
            }
        }

        [Test]
        public void CanHandleItWhenReceiverDisconnectAfterRecieve()
        {
            StartReceiver(x => x.DisconnectAfterMessageSend = true);

            error.ShouldBeOfType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Test]
        public void CanHandleItWhenReceiverSendingBadResponse()
        {
            StartReceiver(x => x.SendBadResponse = true);

            error.ShouldBeOfType<UnexpectedReceivedMessageFormatException>();
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

            error.ShouldBeNull();
            wasSuccessful.ShouldBeTrue();
        }

        [Test]
        public void CanHandleItWhenReceiverSendRevert()
        {
            StartReceiver(x => x.FailOnAcknowledgement = true);

            // this is a case where we create compensation
            // for reported failure on the Receiver side

            error.ShouldBeOfType<RevertSendException>();
            wasSuccessful.ShouldBeTrue();
        }
    }
}