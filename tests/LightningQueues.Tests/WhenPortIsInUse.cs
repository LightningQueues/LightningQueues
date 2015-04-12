using System;
using System.Net;
using FubuTestingSupport;
using LightningQueues.Exceptions;
using Xunit;

namespace LightningQueues.Tests
{
    public class WhenPortIsInUse
    {
        [Fact(Skip = "Not on mono")]
        public void ThrowsWhenAlreadyInUse()
        {
            var one = ObjectMother.QueueManager();
            var two = ObjectMother.QueueManager("test2");
            one.Start();
            typeof(EndpointInUseException).ShouldBeThrownBy(two.Start);
        }

        [Fact(Skip = "Not on mono")]
        public void ExceptionMessageIsCorrect()
        {
            var exception = new EndpointInUseException(new IPEndPoint(IPAddress.Loopback, 2200), new Exception());
            exception.Message.ShouldEqual("The endpoint 127.0.0.1:2200 is already in use");
        }
    }
}