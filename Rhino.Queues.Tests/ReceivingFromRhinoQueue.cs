using System;
using System.IO;
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
    public class ReceivingFromRhinoQueue : WithDebugging, IDisposable
    {
        private readonly RhinoQueue queue;

        public ReceivingFromRhinoQueue()
        {
            if (Directory.Exists("test.esent"))
                Directory.Delete("test.esent", true);

            queue = new RhinoQueue(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
            queue.CreateQueues("h");
        }

        #region IDisposable Members

        public void Dispose()
        {
            queue.Dispose();
        }

        #endregion

        [Fact]
        public void CanReceiveFromQueue()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => { },
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (var tx = new TransactionScope())
            {
                var message = queue.Receive("h");
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                Assert.Throws<TimeoutException>(() => queue.Receive("h", TimeSpan.Zero));

                tx.Complete();
            }
        }

        [Fact]
        public void WhenRevertingTransactionMessageGoesBackToQueue()
        {
            new Sender
            {

                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => { },
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (new TransactionScope())
            {
                var message = queue.Receive("h");
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }

            using (new TransactionScope())
            {
                var message = queue.Receive("h");
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
        }

        [Fact]
        public void CanLookupProcessedMessages()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => { },
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using (var tx = new TransactionScope())
            {
                var message = queue.Receive("h");
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }

            Thread.Sleep(100);

            var messages = queue.GetAllProcessedMessages("h");
            Assert.Equal(1, messages.Length);
            Assert.Equal("hello", Encoding.Unicode.GetString(messages[0].Data));
        }

        [Fact]
        public void CanPeekExistingMessages()
        {
            new Sender
            {
                Destination = new Endpoint("localhost", 23456),
                Failure = exception => Assert.False(true),
                Success = () => { },
                Messages = new[]
                {
                    new Message
                    {
                        Id = MessageId.GenerateRandom(),
                        Queue = "h",
                        Data = Encoding.Unicode.GetBytes("hello"),
                        SentAt = DateTime.Now
                    },
                }
            }.Send();

            using(new TransactionScope())
            {
                // force a wait until we receive the message
                queue.Receive("h");
            }

            var messages = queue.GetAllMessages("h");
            Assert.Equal(1, messages.Length);
            Assert.Equal("hello", Encoding.Unicode.GetString(messages[0].Data));
        }
    }
}