using System.Collections.Generic;
using Rhino.Queues.Visualizer.Model;

namespace Rhino.Queues.Visualizer.Services
{
	public interface IMessageRepository
	{
		IList<MessageModel> GetMessages(QueueModel queue);
	}
}