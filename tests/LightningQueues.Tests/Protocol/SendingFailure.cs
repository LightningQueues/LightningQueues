using System;
using System.IO;
using System.Linq;
using Should;
using LightningQueues.Exceptions;
using LightningQueues.Model;
using LightningQueues.Protocol;
using Xunit;
using Assert = Should.Core.Assertions.Assert;

namespace LightningQueues.Tests.Protocol
{
    public class SendingFailure
    {
        private Exception error;
        private bool wasSuccessful;
        private Sender sender;
        private RecordingLogger _logger;

        public SendingFailure()
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

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverDoesNotExists()
        {
            var task = sender.Send();
            var aggregateException = Assert.Throws<AggregateException>(() => task.Wait());
            aggregateException.InnerExceptions.OfType<FailedToConnectException>().Any().ShouldBeTrue();

            wasSuccessful.ShouldBeFalse();
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverConnectAndDisconnect()
        {
            StartReceiver(x => x.DisconnectAfterConnect = true);

            error.ShouldBeType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverDisconnectDuringRecieve()
        {
            StartReceiver(x => x.DisconnectDuringMessageSend = true);

            error.ShouldBeType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        private void StartReceiver(Action<FakeReceiver> receiverAction)
        {
            using (var receiver = new FakeReceiver())
            {
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
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverDisconnectAfterRecieve()
        {
            StartReceiver(x => x.DisconnectAfterMessageSend = true);

            error.ShouldBeType<IOException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverSendingBadResponse()
        {
            StartReceiver(x => x.SendBadResponse = true);

            error.ShouldBeType<UnexpectedReceivedMessageFormatException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Fact(Skip="Not on mono")]
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

        [Fact(Skip="Not on mono")]
        public void CanHandleItWhenReceiverSendRevert()
        {
            StartReceiver(x => x.FailOnAcknowledgement = true);

            // this is a case where we create compensation
            // for reported failure on the Receiver side

            error.ShouldBeType<RevertSendException>();
            wasSuccessful.ShouldBeTrue();
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleConnectTimeouts()
        {
            sender.Timeout = TimeSpan.FromMilliseconds(500);
            StartReceiver(x => x.AcceptConnections = false);

            error.ShouldBeType<FailedToConnectException>();
            error.InnerException.ShouldBeType<TimeoutException>();
            wasSuccessful.ShouldBeFalse();
        }

        [Fact(Skip="Not on mono")]
        public void CanHandleSendTimeouts()
        {
            sender.Timeout = TimeSpan.FromMilliseconds(500);
            StartReceiver(x => x.TimeoutOnReceive = true);

            error.ShouldBeType<TimeoutException>();
            wasSuccessful.ShouldBeFalse();
        }
    }
}