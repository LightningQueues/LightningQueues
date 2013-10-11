using FubuTestingSupport;
using LightningQueues.Exceptions;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class WhenPortIsInUse
    {
        [Test]
        public void ThrowsWhenAlreadyInUse()
        {
            var one = ObjectMother.QueueManager();
            var two = ObjectMother.QueueManager("test2");
            one.Start();
            typeof(EndpointInUseException).ShouldBeThrownBy(two.Start);
        }
    }
}