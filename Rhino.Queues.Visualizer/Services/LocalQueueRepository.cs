using System.Net;
using Rhino.Queues.Visualizer.Model;

namespace Rhino.Queues.Visualizer.Services
{
	public class LocalQueueRepository : IQueueRepository
	{
		private readonly IQueueManagerCache queueManagerCache;

		public LocalQueueRepository(IQueueManagerCache queueManagerCache)
		{
			this.queueManagerCache = queueManagerCache;
		}

		public QueueModel Get(string path, string queueName)
		{
			var queueManager = GetQueueManager(path);
			var queue = new QueueModel { Name = queueName, Path = path };
			foreach (var subQueueName in queueManager.GetSubqueues(queueName))
			{
				var subQueue = new QueueModel { Name = subQueueName };
				queue.AddSubqueue(subQueue);
			}
			return queue;
		}

		private QueueManager GetQueueManager(string path)
		{
			QueueManager queueManager;
			if (queueManagerCache.Has(path))
				queueManager = queueManagerCache.Get(path);
			else
			{
				queueManager = new QueueManager(new IPEndPoint(IPAddress.Loopback, 2201), path);
				queueManagerCache.Add(queueManager);
			}
			return queueManager;
		}
	}
}