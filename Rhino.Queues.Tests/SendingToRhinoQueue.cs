using System;
using System.IO;
using System.Net;
using System.Transactions;
using Rhino.Queues.Protocol;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
    public class SendingToRhinoQueue : WithDebugging, IDisposable
    {
        private readonly RhinoQueue sender, receiver;

        public SendingToRhinoQueue()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            if (Directory.Exists("test2.esent"))
                Directory.Delete("test2.esent", true);

            sender = new RhinoQueue(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            receiver = new RhinoQueue(new IPEndPoint(IPAddress.Loopback, 23457), "test2.esent");
            receiver.CreateQueues("h", "a");
        }

        [Fact]
        public void CanSendToQueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 1, 2, 4, 5 });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");

                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
        public void CanLookAtSentMessages()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 1, 2, 4, 5 });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                receiver.Receive("h");
                tx.Complete();
            }

            var messages = sender.GetAllSentMessages();
            Assert.Equal(1, messages.Length);
            Assert.Equal(new byte[] {1, 2, 4, 5}, messages[0].Data);
        }

        [Fact]
        public void CanLookAtMessagesCurrentlySending()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 1312), "h", new byte[] { 1, 2, 4, 5 });

                tx.Complete();
            }

            var messages = sender.GetMessagesCurrentlySending();
            Assert.Equal(1, messages.Length);
            Assert.Equal(new byte[] { 1, 2, 4, 5 }, messages[0].Data);
        }

        [Fact]
        public void WillNotSendIfTxIsNotCommitted()
        {
            using (new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 1, 2, 4, 5 });
            }

            using (var tx = new TransactionScope())
            {
                Assert.Throws<TimeoutException>(() => receiver.Receive("h", TimeSpan.FromSeconds(1)));

                tx.Complete();
            }
        }

        [Fact]
        public void CanSendSeveralMessagesToQueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 1, 2, 4, 5 });
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 4, 5, 6, 7 });
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 6, 7, 8, 9 });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");
                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                message = receiver.Receive("h");
                Assert.Equal(new byte[] { 4, 5, 6, 7 }, message.Data);

                message = receiver.Receive("h");
                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                tx.Complete();
            }
        }

        [Fact]
        public void CanSendToSeveralQueues()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 1, 2, 4, 5 });
                sender.Send(new Endpoint("localhost", 23457), "a", new byte[] { 4, 5, 6, 7 });
                sender.Send(new Endpoint("localhost", 23457), "h", new byte[] { 6, 7, 8, 9 });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");
                Assert.Equal(new byte[] { 1, 2, 4, 5 }, message.Data);

                message = receiver.Receive("h");
                Assert.Equal(new byte[] { 6, 7, 8, 9 }, message.Data);

                message = receiver.Receive("a");
                Assert.Equal(new byte[] { 4, 5, 6, 7 }, message.Data);

                tx.Complete();
            }
        }

        public void Dispose()
        {
            sender.Dispose();
            receiver.Dispose();
        }
    }
}