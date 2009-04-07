using System;
using Caliburn.MVP.Models;

namespace Rhino.Queues.Visualizer.Model
{
	public class MessageModel : ModelBase
	{
		private static readonly IPropertyDefinition<string> DataProperty =
			Property<MessageModel, string>(x => x.Data);

		public string Data
		{
			get { return GetValue(DataProperty); }
			set { SetValue(DataProperty, value); }
		}

		private static readonly IPropertyDefinition<int> LengthProperty =
			Property<MessageModel, int>(x => x.Length);

		public int Length
		{
			get { return GetValue(LengthProperty); }
			set { SetValue(LengthProperty, value); }
		}

		private static readonly IPropertyDefinition<string> MessageIdProperty =
			Property<MessageModel, string>(x => x.MessageId);

		public string MessageId
		{
			get { return GetValue(MessageIdProperty); }
			set { SetValue(MessageIdProperty, value); }
		}

		private static readonly IPropertyDefinition<DateTime> SentAtProperty =
			Property<MessageModel, DateTime>(x => x.SentAt);

		public DateTime SentAt
		{
			get { return GetValue(SentAtProperty); }
			set { SetValue(SentAtProperty, value); }
		}

		private static readonly IPropertyDefinition<string> QueueNameProperty =
			Property<MessageModel, string>(x => x.QueueName);

		public string QueueName
		{
			get { return GetValue(QueueNameProperty); }
			set { SetValue(QueueNameProperty, value); }
		}

		private static readonly IPropertyDefinition<string> SubQueueNameProperty =
			Property<MessageModel, string>(x => x.SubQueueName);

		public string SubQueueName
		{
			get { return GetValue(SubQueueNameProperty); }
			set { SetValue(SubQueueNameProperty, value); }
		}
	}
}