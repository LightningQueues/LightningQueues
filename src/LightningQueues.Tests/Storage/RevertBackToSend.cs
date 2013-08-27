using System;
using System.Collections.Specialized;
using System.IO;
using FubuTestingSupport;
using LightningQueues.Protocol;
using LightningQueues.Storage;
using NUnit.Framework;

namespace LightningQueues.Tests.Storage
{
    [TestFixture]
    public class RevertBackToSend
    {
        [SetUp]
        public void Setup()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);
        }

        [Test]
        public void MovesMessageToOutgoingFromHistory()
        {
            using (var qf = new QueueStorage("test.esent", new QueueManagerConfiguration(), ObjectMother.Logger()))
            {
                qf.Initialize();
                qf.Global(actions =>
                {
                    actions.CreateQueueIfDoesNotExists("test");
                    actions.Commit();
                });

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
                    actions.Commit();
                });

                qf.Send(actions =>
                {
                    Endpoint endpoint;
                    var msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, out endpoint);
                    var bookmark = actions.MarkOutgoingMessageAsSuccessfullySent(msgs[0].Bookmark);
                    actions.RevertBackToSend(new[] { bookmark });

                    msgs = actions.GetMessagesToSendAndMarkThemAsInFlight(int.MaxValue, int.MaxValue, out endpoint);
                    msgs.ShouldNotBeEmpty();
                    actions.Commit();
                });

                qf.Global(actions =>
                {
                    var messages = actions.GetSentMessages();
                    messages.ShouldHaveCount(0);
                    actions.Commit();
                });
            }
        }
    }
}