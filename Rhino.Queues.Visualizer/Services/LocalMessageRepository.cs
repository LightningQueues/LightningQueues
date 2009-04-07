using System.Collections.Generic;
using System.Text;
using Rhino.Queues.Visualizer.Model;

namespace Rhino.Queues.Visualizer.Services
{
	public class LocalMessageRepository : IMessageRepository
	{
		private readonly IQueueManagerCache queueManagerCache;

		public LocalMessageRepository(IQueueManagerCache queueManagerCache)
		{
			this.queueManagerCache = queueManagerCache;
		}

		public IList<MessageModel> GetMessages(QueueModel queue)
		{
			var path = queue.ParentQueueModel == null ? queue.Path : queue.ParentQueueModel.Path;
			var queueName = queue.ParentQueueModel == null ? queue.Name : queue.ParentQueueModel.Name;
			var subQueueName = queue.ParentQueueModel == null ? null : queue.Name;
			var queueManager = queueManagerCache.Get(path);
			
			var results = new List<MessageModel>();
			foreach (var message in queueManager.GetAllMessages(queueName, subQueueName))
			{
				results.Add(new MessageModel
				{
					Data = Encoding.UTF8.GetString(message.Data),
					Length = message.Data.Length,
					SentAt = message.SentAt,
					MessageId = message.Id.ToString(),
					QueueName = message.Queue,
					SubQueueName = subQueueName,
				});
			}
			return results;
		}
	}
}