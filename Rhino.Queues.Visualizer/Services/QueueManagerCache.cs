using System.Collections.Generic;

namespace Rhino.Queues.Visualizer.Services
{
	public class QueueManagerCache : IQueueManagerCache
	{
		private readonly IDictionary<string, QueueManager> cache = new Dictionary<string, QueueManager>();

		public bool Has(string path)
		{
			return cache.ContainsKey(path);
		}

		public void Add(QueueManager queueManager)
		{
			cache.Add(queueManager.Path, queueManager);
		}

		public QueueManager Get(string path)
		{
			return cache[path];
		}
	}
}