using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using FubuTestingSupport;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Storage;
using NUnit.Framework;

namespace LightningQueues.Tests.Storage
{
    [TestFixture]
    public class DeliveryOptions
    {
        [Test]
        public void MovesExpiredMessageToOutgoingHistory()
        {
            using (var qf = new QueueStorage("test.esent", new QueueManagerConfiguration(), ObjectMother.Logger()))
            {
                qf.Initialize();

                qf.Global(actions => actions.CreateQueueIfDoesNotExists("test"));

                var testMessage = new MessagePayload{
                    Data = new byte[0],
                    DeliverBy = DateTime.Now.AddSeconds(-1),
                    Headers = new NameValueCollection(),
                    MaxAttempts = null
                };

                Guid messageId = Guid.Empty;
                qf.Global(actions =>
                {
                    Guid transactionId = Guid.NewGuid();
                    messageId = actions.RegisterToSend(new Endpoint("localhost", 0), "test", null, testMessage, transactionId);
                    actions.MarkAsReadyToSend(transactionId);
                });

                qf.Send(actions =>
                {
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, new Endpoint("localhost", 0));
                    msgs.ShouldHaveCount(0);
                });

                qf.Global(actions =>
                {
                    var message = actions.GetSentMessages().FirstOrDefault(x => x.Id.MessageIdentifier == messageId);
                    message.ShouldNotBeNull();
                    OutgoingMessageStatus.Failed.ShouldEqual(message.OutgoingStatus);
                });
            }
        }

        [Test]
        public void MovesMessageToOutgoingHistoryAfterMaxAttempts()
        {
            Directory.Delete("test.esent", true);
            using (var qf = new QueueStorage("test.esent", new QueueManagerConfiguration(), ObjectMother.Logger()))
            {
                qf.Initialize();
                qf.Global(actions => actions.CreateQueueIfDoesNotExists("test"));

                var testMessage = new MessagePayload
                {
                    Data = new byte[0],
                    DeliverBy = null,
                    Headers = new NameValueCollection(),
                    MaxAttempts = 1
                };

                Guid messageId = Guid.Empty;
                qf.Global(actions =>
                {
                    Guid transactionId = Guid.NewGuid();
                    messageId = actions.RegisterToSend(new Endpoint("localhost", 0),
                        "test",
                        null,
                        testMessage,
                        transactionId);
                    actions.MarkAsReadyToSend(transactionId);
                });

                qf.Send(actions =>
                {
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, new Endpoint("localhost", 0));
                    actions.MarkOutgoingMessageAsFailedTransmission(msgs.First().Bookmark, false);
                    
                    msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, new Endpoint("localhost", 0));
                    msgs.ShouldHaveCount(0);
                });

                qf.Global(actions =>
                {
                    PersistentMessageToSend message = actions.GetSentMessages().FirstOrDefault(x => x.Id.MessageIdentifier == messageId);
                    message.ShouldNotBeNull();
                    OutgoingMessageStatus.Failed.ShouldEqual(message.OutgoingStatus);
                });
            }
        }
    }
}