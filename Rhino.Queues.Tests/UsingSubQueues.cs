using System;
using System.IO;
using System.Net;
using System.Text;
using System.Transactions;
using Rhino.Queues.Tests.Protocol;
using Xunit;

namespace Rhino.Queues.Tests
{
	public class UsingSubQueues : WithDebugging, IDisposable
	{
		private readonly QueueManager sender, receiver;

		public UsingSubQueues()
		{
			if (Directory.Exists("test.esent"))
				Directory.Delete("test.esent", true);

			if (Directory.Exists("test2.esent"))
				Directory.Delete("test2.esent", true);

			sender = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23456), "test.esent");
			receiver = new QueueManager(new IPEndPoint(IPAddress.Loopback, 23457), "test2.esent");
			receiver.CreateQueues("h", "a");
		}

		[Fact]
		public void Can_send_and_receive_subqueue()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h/a"),
					new MessagePayload
					{
						Data = Encoding.Unicode.GetBytes("subzero")
					});

				tx.Complete();
			}

			using (var tx = new TransactionScope())
			{
				var message = receiver.Receive("h", "a");

				Assert.Equal("subzero", Encoding.Unicode.GetString(message.Data));

				tx.Complete();
			}
		}

		[Fact]
		public void Can_move_msg_to_subqueue()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h"),
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

				Assert.Equal("subzero", Encoding.Unicode.GetString(message.Data));

				tx.Complete();
			}
		}

		[Fact]
		public void Moving_to_subqueue_move_from_main_queue()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h"),
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
				Assert.NotNull(receiver.Receive("h", "b"));

				Assert.Throws<TimeoutException>(() => receiver.Receive("h", TimeSpan.FromSeconds(1)));

				tx.Complete();
			}
		}

		[Fact]
		public void Moving_to_subqueue_will_not_be_completed_until_tx_is_completed()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h"),
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

		[Fact]
		public void Moving_to_subqueue_will_be_reverted_by_transaction_rollback()
		{
			using (var tx = new TransactionScope())
			{
				sender.Send(
					new Uri("rhino.queues://localhost:23457/h"),
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

				Assert.NotNull(message);

				tx.Complete();
			}
		}

		[Fact]
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
			Assert.Equal(1, messages.Length);
			Assert.Equal("1234", Encoding.Unicode.GetString(messages[0].Data));

			messages = receiver.GetAllMessages("h", "c");
			Assert.Equal(1, messages.Length);
			Assert.Equal("4321", Encoding.Unicode.GetString(messages[0].Data));
		}

		[Fact]
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
			Assert.Equal(new[] { "b", "c", "u" }, q.GetSubqeueues());
		}

		[Fact]
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

			Assert.Equal(4, receiver.GetNumberOfMessages("h"));
			Assert.Equal(4, receiver.GetNumberOfMessages("h"));
		}

		public void Dispose()
		{
			sender.Dispose();
			receiver.Dispose();
		}
	}
}