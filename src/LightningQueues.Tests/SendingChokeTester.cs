using FubuCore;
using FubuTestingSupport;
using NUnit.Framework;

namespace LightningQueues.Tests
{
    [TestFixture]
    public class SendingChokeTester
    {
        [Test]
        public void the_defaults()
        {
            var choke = new SendingChoke();
            choke.MaxConnectingCount.ShouldEqual(30);
            choke.MaxSendingCount.ShouldEqual(5);
        }

        [Test]
        public void choke_should_send_returns_true_if_havent_sent_anything()
        {
            var choke = new SendingChoke();
            choke.ShouldBeginSend().ShouldBeTrue();
        }

        [Test]
        public void choke_returns_false_if_at_max_sending_count()
        {
            var choke = new SendingChoke();
            5.Times(x =>
            {
                choke.StartSend();
                choke.SuccessfullyConnected();
            });
            choke.ShouldBeginSend().ShouldBeFalse();
        }

        [Test]
        public void choke_returns_true_if_several_connecting_but_not_connected_yet_up_to_maximum_connecting_count()
        {
            var choke = new SendingChoke();
            4.Times(x =>
            {
                choke.StartSend();
                choke.SuccessfullyConnected();
            });
            30.Times(x =>
            {
                choke.StartSend();
            });
            choke.ShouldBeginSend().ShouldBeTrue();
        }

        [Test]
        public void choke_returns_false_if_max_sending_isnt_exceeded_but_max_connecting_is()
        {
            var choke = new SendingChoke();
            2.Times(x =>
            {
                choke.StartSend();
                choke.SuccessfullyConnected();
            });
            31.Times(x => choke.StartSend());
            choke.ShouldBeginSend().ShouldBeFalse();
        }

        [Test]
        public void can_alter_max_connecting_count()
        {
            var choke = new SendingChoke();
            choke.AlterConnectingCountMaximumTo(10);
            11.Times(x => choke.StartSend());
            choke.ShouldBeginSend().ShouldBeFalse();
        }

        [Test]
        public void can_alter_max_sending_count()
        {
            var choke = new SendingChoke();
            choke.AlterSendingCountMaximumTo(10);
            5.Times(x =>
            {
                choke.StartSend();
                choke.SuccessfullyConnected();
            });
            choke.ShouldBeginSend().ShouldBeTrue();
            5.Times(x =>
            {
                choke.StartSend();
                choke.SuccessfullyConnected();
            });
            choke.ShouldBeginSend().ShouldBeFalse();
        }
    }
}