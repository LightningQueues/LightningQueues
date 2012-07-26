using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading;
using Rhino.Queues.Protocol;
using Rhino.Queues.Storage;
using Xunit;

namespace Rhino.Queues.Tests.Storage
{
    public class DeliveryOptions
    {
        [Fact]
        public void MovesExpiredMessageToOutgoingHistory()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();

                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("test");
                    actions.Commit();
                });

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
                    actions.Commit();
                });

                qf.Send(actions =>
                {
                    Endpoint endpoint;
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, out endpoint);
                    Assert.Empty(msgs);
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var message = actions.GetSentMessages().FirstOrDefault(x => x.Id.MessageIdentifier == messageId);
                    Assert.NotNull(message);
                    Assert.Equal(OutgoingMessageStatus.Failed, message.OutgoingStatus);
                    actions.Commit();
                });
            }
        }

        [Fact]
        public void MovesMessageToOutgoingHistoryAfterMaxAttempts()
        {
            using (var qf = new QueueStorage("test.esent"))
            {
                qf.Initialize();

                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("test");
                    actions.Commit();
                });

                var testMessage = new MessagePayload{
                    Data = new byte[0],
                    DeliverBy = null,
                    Headers = new NameValueCollection(),
                    MaxAttempts = 1
                };

                Guid messageId = Guid.Empty;
                qf.Global(actions =>
                {
                    Guid transactionId = Guid.NewGuid();
                    messageId = actions.RegisterToSend(new Endpoint("localhost", 0), "test", null, testMessage, transactionId);
                    actions.MarkAsReadyToSend(transactionId);
                    actions.Commit();
                });

                qf.Send(actions =>
                {
                    Endpoint endpoint;
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, out endpoint);
                    actions.MarkOutgoingMessageAsFailedTransmission(msgs.First().Bookmark, false);

                    // HACK: ESENT updates asynchronously, so we need to give it a chance to finish.
                    Thread.Sleep(1000);

                    msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, out endpoint);
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var message = actions.GetSentMessages().FirstOrDefault(x => x.Id.MessageIdentifier == messageId);
                    Assert.NotNull(message);
                    Assert.Equal(OutgoingMessageStatus.Failed, message.OutgoingStatus);
                    actions.Commit();
                });
            }
        }
    }
}