using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Transactions;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    class RaisingSendEvents : WithDebugging, IDisposable
    {
        private const string TEST_QUEUE_1 = "test.esent";
        private const string TEST_QUEUE_2 = "test2.esent";

        private QueueManager sender, receiver;

        private MessageEventArgs messageEventArgs;

        public void Setup()
        {
            if (Directory.Exists(TEST_QUEUE_1))
                Directory.Delete(TEST_QUEUE_1, true);

            if (Directory.Exists(TEST_QUEUE_2))
                Directory.Delete(TEST_QUEUE_2, true);

            sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), TEST_QUEUE_1);
            messageEventArgs = null;
        }

        void RecordMessageEvent(object s, MessageEventArgs e)
        {
            messageEventArgs = e;
        }

        [Fact]
        public void MessageQueuedForSend_EventIsRaised()
        {
            Setup();

            sender.MessageQueuedForSend += RecordMessageEvent;

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23999/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                tx.Complete();
            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("localhost", messageEventArgs.Endpoint.Host);
            Assert.Equal(23999, messageEventArgs.Endpoint.Port);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageQueuedForSend_EventIsRaised_EvenIfTransactionFails()
        {
            Setup();

            sender.MessageQueuedForSend += RecordMessageEvent;

            using (new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23999/h"),
                    new MessagePayload
                        {
                            Data = new byte[] { 1, 2, 4, 5 }
                        });

            }

            Assert.NotNull(messageEventArgs);
            Assert.Equal("localhost", messageEventArgs.Endpoint.Host);
            Assert.Equal(23999, messageEventArgs.Endpoint.Port);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageSent_EventIsRaised()
        {
            Setup();

            sender.MessageSent += RecordMessageEvent;

            receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2);
            receiver.CreateQueues("h");

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                tx.Complete();
            }

            Thread.Sleep(1000);

            Assert.NotNull(messageEventArgs);
            Assert.Equal("localhost", messageEventArgs.Endpoint.Host);
            Assert.Equal(23457, messageEventArgs.Endpoint.Port);
            Assert.Equal("h", messageEventArgs.Message.Queue);
        }

        [Fact]
        public void MessageSent_EventNotRaised_IfNotSent()
        {
            Setup();

            sender.MessageSent += RecordMessageEvent;

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23999/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                tx.Complete();
            }

            Thread.Sleep(1000);

            Assert.Null(messageEventArgs);
        }

        [Fact]
        public void MessageSent_EventNotRaised_IfReceiverReverts()
        {
            Setup();

            sender.MessageSent += RecordMessageEvent;

            receiver = new RevertingQueueManager(new IPEndPoint(IPAddress.Loopback, 23457), TEST_QUEUE_2);
            receiver.CreateQueues("h");

            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("rhino.queues://localhost:23457/h"),
                     new MessagePayload
                     {
                         Data = new byte[] { 1, 2, 4, 5 }
                     });

                tx.Complete();
            }

            Thread.Sleep(1000);

            Assert.Null(messageEventArgs);
        }

        private class RevertingQueueManager : QueueManager
        {
            public RevertingQueueManager(IPEndPoint endpoint, string path)
                : base(endpoint, path)
            {
            }

            protected override IMessageAcceptance AcceptMessages(Message[] msgs)
            {
                throw new Exception("Cannot accept messages.");
            }
        }

        public void Dispose()
        {
            if (sender != null) sender.Dispose();
            if (receiver != null) receiver.Dispose();
        }
    }
}