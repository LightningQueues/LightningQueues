using System;
using System.Text;
using System.Transactions;
using FubuTestingSupport;
using Xunit;

namespace LightningQueues.Tests
{
    public class UsingSubQueues : IDisposable
    {
        private QueueManager sender, receiver;

        public UsingSubQueues()
        {
            sender = ObjectMother.QueueManager();
            sender.Start();

            receiver = ObjectMother.QueueManager("test2", 23457);
            receiver.CreateQueues("a");
            receiver.Start();
        }

        [Fact(Skip="Not on mono")]
        public void Can_send_and_receive_subqueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h/a"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", "a");

                "subzero".ShouldEqual(Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Can_remove_and_move_msg_to_subqueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");

                receiver.MoveTo("b", message);

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h", "b");

                "subzero".ShouldEqual(Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Can_peek_and_move_msg_to_subqueue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            var message = receiver.Peek("h");

            using (var tx = new TransactionScope())
            {
                receiver.MoveTo("b", message);

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                message = receiver.Receive("h", "b");

                "subzero".ShouldEqual(Encoding.Unicode.GetString(message.Data));

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Moving_to_subqueue_should_remove_from_main_queue()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            var message = receiver.Peek("h");

            using (var tx = new TransactionScope())
            {

                receiver.MoveTo("b", message);

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                receiver.Receive("h", "b").ShouldNotBeNull();

                Assert.Throws<TimeoutException>(() => receiver.Receive("h", TimeSpan.FromSeconds(1)));

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Moving_to_subqueue_will_not_be_completed_until_tx_is_completed()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");

                receiver.MoveTo("b", message);

                Assert.Throws<TimeoutException>(() => receiver.Receive("h", "b", TimeSpan.FromSeconds(1)));

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Moving_to_subqueue_will_be_reverted_by_transaction_rollback()
        {
            using (var tx = new TransactionScope())
            {
                sender.Send(
                    new Uri("lq.tcp://localhost:23457/h"),
                    new MessagePayload
                    {
                        Data = Encoding.Unicode.GetBytes("subzero")
                    });

                tx.Complete();
            }

            using (new TransactionScope())
            {
                var message = receiver.Receive("h");

                receiver.MoveTo("b", message);

            }

            using (var tx = new TransactionScope())
            {
                var message = receiver.Receive("h");

                message.ShouldNotBeNull();

                tx.Complete();
            }
        }

        [Fact(Skip="Not on mono")]
        public void Can_scan_messages_in_main_queue_without_seeing_messages_from_subqueue()
        {
            using (var tx = new TransactionScope())
            {
                receiver.EnqueueDirectlyTo("h", null, new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("1234")
                });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                tx.Complete();
            }

            var messages = receiver.GetAllMessages("h", null);
            1.ShouldEqual(messages.Length);
            "1234".ShouldEqual(Encoding.Unicode.GetString(messages[0].Data));

            messages = receiver.GetAllMessages("h", "c");
            1.ShouldEqual(messages.Length);
            "4321".ShouldEqual(Encoding.Unicode.GetString(messages[0].Data));
        }

        [Fact(Skip="Not on mono")]
        public void Can_get_list_of_subqueues()
        {
            using (var tx = new TransactionScope())
            {
                receiver.EnqueueDirectlyTo("h", "b", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("1234")
                });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                receiver.EnqueueDirectlyTo("h", "u", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                tx.Complete();
            }

            var q = receiver.GetQueue("h");
            new[] { "b", "c", "u" }.ShouldEqual(q.GetSubqeueues());
        }

        [Fact(Skip="Not on mono")]
        public void Can_get_number_of_messages()
        {
            using (var tx = new TransactionScope())
            {
                receiver.EnqueueDirectlyTo("h", "b", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("1234")
                });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                receiver.EnqueueDirectlyTo("h", "c", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                receiver.EnqueueDirectlyTo("h", "u", new MessagePayload
                {
                    Data = Encoding.Unicode.GetBytes("4321")
                });
                tx.Complete();
            }

            4.ShouldEqual(receiver.GetNumberOfMessages("h"));
            4.ShouldEqual(receiver.GetNumberOfMessages("h"));
        }

        public void Dispose()
        {
            sender.Dispose();
            receiver.Dispose();
        }
    }
}