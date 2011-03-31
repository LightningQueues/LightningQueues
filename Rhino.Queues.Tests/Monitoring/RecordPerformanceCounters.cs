using System;
using System.Net;
using System.Transactions;
using Rhino.Mocks;
using Rhino.Queues.Model;
using Rhino.Queues.Monitoring;
using Rhino.Queues.Protocol;
using Xunit;

namespace Rhino.Queues.Tests.Monitoring
{
    class RecordPerformanceCounters
    {
        private TestablePerformanceMonitor performanceMonitor;
        private IQueueManager queueManager;

        private void Setup()
        {
            queueManager = MockRepository.GenerateStub<IQueueManager>();
            queueManager.Stub(qm => qm.Queues).Return(new string[0]);
            queueManager.Stub(qm => qm.GetMessagesCurrentlySending()).Return(new PersistentMessageToSend[0]);
            queueManager.Stub(qm => qm.Endpoint).Return(new IPEndPoint(IPAddress.Loopback, 123) );

            performanceMonitor = new TestablePerformanceMonitor(queueManager);
        }

        [Fact]
        public void MessageQueuedForSend_should_updatet_correct_instance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageQueuedForSend += null,  
                new Message { Queue = "q" }, 
                "localhost:123/q");
        }

        [Fact]
        public void MessageSent_should_updatet_correct_instance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageSent += null,  
                new Message { Queue = "q" }, 
                "localhost:123/q");
        }

        [Fact]
        public void MessageQueuedForReceive_should_updatet_correct_instance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageQueuedForReceive += null,  
                new Message { Queue = "q" }, 
                "127.0.0.1:123/q");
        }

        [Fact]
        public void MessageReceived_should_updatet_correct_instance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageReceived += null,  
                new Message { Queue = "q" },
                "127.0.0.1:123/q");
        }

        private void TestEventUpdatesCorrectInstance(Action<IQueueManager> @event, Message message, string expectedInstanceName)
        {
            Setup();

            var e = new MessageEventArgs(new Endpoint("localhost", 123), message);
            queueManager.Raise(@event, null, e);

            Assert.Equal(expectedInstanceName, performanceMonitor.InstanceName);
        }


        [Fact]
        public void MessageQueuedForSend_without_transaction_should_increment_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 0;
            
            var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageQueuedForSend += null, null, e);

            Assert.Equal(1, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageQueuedForSend_in_committed_transaction_should_increment_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 0;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message {Queue = "q"});
                queueManager.Raise(qm => qm.MessageQueuedForSend += null, null, e);

                tx.Complete();
            }

            Assert.Equal(1, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageQueuedForSend_in_transaction_should_not_increment_UnsentMessages_prior_to_commit()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 0;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message {Queue = "q"});
                queueManager.Raise(qm => qm.MessageQueuedForSend += null, null, e);

                Assert.Equal(0, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);

                tx.Complete();
            }
        }

        [Fact]
        public void MessageQueuedForSend_in_failed_transaction_should_not_increment_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 0;

            using (new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message {Queue = "q"});
                queueManager.Raise(qm => qm.MessageQueuedForSend += null, null, e);
            }

            Assert.Equal(0, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageSent_without_transaction_should_decrement_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 1;
            
            var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageSent += null, null, e);

            Assert.Equal(0, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageSent_in_committed_transaction_should_decrement_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 1;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageSent += null, null, e); 

                tx.Complete();
            }

            Assert.Equal(0, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageSent_in_transaction_should_not_decrement_UnsentMessages_prior_to_commit()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 1;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageSent += null, null, e);

                Assert.Equal(1, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);

                tx.Complete();
            }
        }

        [Fact]
        public void MessageSent_in_failed_transaction_should_not_decrement_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 1;

            using (new TransactionScope())
            {
                var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageSent += null, null, e); 
            }

            Assert.Equal(1, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageQueuedForReceive_without_transaction_should_increment_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 0;
            
            var e = new MessageEventArgs(null, new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageQueuedForReceive += null, null, e);

            Assert.Equal(1, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageQueuedForReceive_in_committed_transaction_should_increment_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 0;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageQueuedForReceive += null, null, e);
                
                tx.Complete();
            }

            Assert.Equal(1, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageQueuedForReceive_in_transaction_not_should_increment_ArrivedMessages_prior_to_commit()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 0;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageQueuedForReceive += null, null, e);
                
                Assert.Equal(0, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);

                tx.Complete();
            }
        }

        [Fact]
        public void MessageQueuedForReceive_in_failed_transaction_not_should_increment_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 0;

            using (new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageQueuedForReceive += null, null, e);
            }

            Assert.Equal(0, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageReceived_without_transaction_should_decrement_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 1;
            
            var e = new MessageEventArgs(null, new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageReceived += null, null, e);

            Assert.Equal(0, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageReceived_in_committed_transaction_should_decrement_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 1;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageReceived += null, null, e);
                
                tx.Complete();
            }

            Assert.Equal(0, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageReceived_in_transaction_should_not_decrement_ArrivedMessages_prior_to_commit()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 1;

            using (var tx = new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageReceived += null, null, e);

                Assert.Equal(1, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
                
                tx.Complete();
            }
        }

        [Fact]
        public void MessageReceived_in_failed_transaction_should_not_decrement_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 1;

            using (new TransactionScope())
            {
                var e = new MessageEventArgs(null, new Message { Queue = "q" });
                queueManager.Raise(qm => qm.MessageReceived += null, null, e);
            }

            Assert.Equal(1, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        private class TestablePerformanceMonitor : PerformanceMonitor
        {
            private readonly TestProvider testProvider= new TestProvider();
            public TestablePerformanceMonitor(IQueueManager queueManager) : base(queueManager)
            {

            }

            public string InstanceName { get { return testProvider.InstanceName; } }
            public IOutboundPerfomanceCounters OutboundPerfomanceCounters { get { return testProvider.OutboundPerfomanceCounters; } }
            public IInboundPerfomanceCounters InboundPerfomanceCounters { get { return testProvider.InboundPerfomanceCounters; } }

            protected override IPerformanceCounterProvider ImmediatelyRecordingProvider
            {
                get { return testProvider; }
            }

            private class TestProvider : IPerformanceCounterProvider
            {
                public string InstanceName;
                public readonly IInboundPerfomanceCounters InboundPerfomanceCounters = MockRepository.GenerateStub<IInboundPerfomanceCounters>();
                public readonly IOutboundPerfomanceCounters OutboundPerfomanceCounters = MockRepository.GenerateStub<IOutboundPerfomanceCounters>();

                public IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
                {
                    InstanceName = instanceName;
                    return OutboundPerfomanceCounters;
                }

                public IInboundPerfomanceCounters GetInboundCounters(string instanceName)
                {
                    InstanceName = instanceName;
                    return InboundPerfomanceCounters;
                }
            }
        }
    }

}
