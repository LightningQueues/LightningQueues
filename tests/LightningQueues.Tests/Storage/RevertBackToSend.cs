using System;
using System.Collections.Specialized;
using System.IO;
using Should;
using LightningQueues.Protocol;
using LightningQueues.Storage;
using Xunit;

namespace LightningQueues.Tests.Storage
{
    public class RevertBackToSend
    {
        public RevertBackToSend()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);
        }

        [Fact(Skip="Not on mono")]
        public void MovesMessageToOutgoingFromHistory()
        {
            using (var qf = new QueueStorage("test.esent", new QueueManagerConfiguration()))
            {
                qf.Initialize();
                qf.Global(actions => actions.CreateQueueIfDoesNotExists("test"));

                var testMessage = new MessagePayload
                {
                    Data = new byte[0],
                    Headers = new NameValueCollection(),
                };

                qf.Global(actions =>
                {
                    Guid transactionId = Guid.NewGuid();
                    actions.RegisterToSend(new Endpoint("localhost", 0),
                        "test",
                        null,
                        testMessage,
                        transactionId);
                    actions.MarkAsReadyToSend(transactionId);
                });

                qf.Send(actions =>
                {
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, new Endpoint("localhost", 0));
                    var bookmark = actions.MarkOutgoingMessageAsSuccessfullySent(msgs[0].Bookmark);
                    actions.RevertBackToSend(new[] { bookmark });

                    msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, new Endpoint("localhost", 0));
                    msgs.ShouldNotBeNull();
                    msgs.ShouldBeEmpty();
                });

                qf.Global(actions =>
                {
                    var messages = actions.GetSentMessages();
                    messages.ShouldBeEmpty();
                });
            }
        }
    }
}