using Rhino.Queues.Visualizer.Model;

namespace Rhino.Queues.Visualizer.Services
{
	public interface IQueueRepository
	{
		QueueModel Get(string path, string queueName);
	}
}