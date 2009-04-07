namespace Rhino.Queues.Visualizer.Services
{
	public interface IQueueManagerCache
	{
		bool Has(string path);
		void Add(QueueManager queueManager);
		QueueManager Get(string path);
	}
}