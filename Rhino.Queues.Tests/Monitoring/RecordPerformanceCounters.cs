using System;
using System.Net;
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
        public void MessageQueuedForSend_ShouldUpdatetCorrectInstance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageQueuedForSend += null,  
                new Message { Queue = "q" }, 
                "localhost:123/q");
        }

        [Fact]
        public void MessageSent_ShouldUpdatetCorrectInstance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageSent += null,  
                new Message { Queue = "q" }, 
                "localhost:123/q");
        }

        [Fact]
        public void MessageQueuedForReceive_ShouldUpdatetCorrectInstance()
        {
            TestEventUpdatesCorrectInstance(
                qm => qm.MessageQueuedForReceive += null,  
                new Message { Queue = "q" }, 
                "127.0.0.1:123/q");
        }

        [Fact]
        public void MessageReceived_ShouldUpdatetCorrectInstance()
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
        public void MessageQueuedForSend_ShouldIncrement_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 0;
            
            var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageQueuedForSend += null, null, e);

            Assert.Equal(1, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageSent_ShouldDecrement_UnsentMessages()
        {
            Setup();

            performanceMonitor.OutboundPerfomanceCounters.UnsentMessages = 1;
            
            var e = new MessageEventArgs(new Endpoint("localhost", 123), new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageSent += null, null, e);

            Assert.Equal(0, performanceMonitor.OutboundPerfomanceCounters.UnsentMessages);
        }

        [Fact]
        public void MessageQueuedForReceive_ShouldIncrement_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 0;
            
            var e = new MessageEventArgs(null, new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageQueuedForReceive += null, null, e);

            Assert.Equal(1, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        [Fact]
        public void MessageReceived_ShouldDecrement_ArrivedMessages()
        {
            Setup();

            performanceMonitor.InboundPerfomanceCounters.ArrivedMessages = 1;
            
            var e = new MessageEventArgs(null, new Message { Queue = "q" });
            queueManager.Raise(qm => qm.MessageReceived += null, null, e);

            Assert.Equal(0, performanceMonitor.InboundPerfomanceCounters.ArrivedMessages);
        }

        private class TestablePerformanceMonitor : PerformanceMonitor
        {
            public string InstanceName;
            public readonly IInboundPerfomanceCounters InboundPerfomanceCounters = MockRepository.GenerateStub<IInboundPerfomanceCounters>();
            public readonly IOutboundPerfomanceCounters OutboundPerfomanceCounters = MockRepository.GenerateStub<IOutboundPerfomanceCounters>();

            public TestablePerformanceMonitor(IQueueManager queueManager) : base(queueManager)
            {
            }

            protected override IInboundPerfomanceCounters GetInboundCounters(string instanceName)
            {
                InstanceName = instanceName;
                return InboundPerfomanceCounters;
            }

            protected override IOutboundPerfomanceCounters GetOutboundCounters(string instanceName)
            {
                InstanceName = instanceName;
                return OutboundPerfomanceCounters;
            }
        }
    }

}
