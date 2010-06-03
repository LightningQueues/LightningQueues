<Query Kind="Statements">
  <Reference>C:\Projects\temp\Rhino.Queues.dll</Reference>
  <Namespace>Rhino.Queues.Storage</Namespace>
  <Namespace>Rhino.Queues.Model</Namespace>
</Query>

//ATTN: you must right click in this query window and choose Query Properties then add a reference to Rhino.Queues.dll
//PersistentMessage contains the following properties Id, Queue, SentAt, Headers, Data, SubQueue, Bookmark, Status
string pathToQueue = @"Path to folder.esent";
var messages = new List<object>();
Action<IEnumerable<PersistentMessage>> messageSelectAction = msgs => 
	messages.AddRange(msgs.Select(m => new {m.Queue, m.SubQueue, m.SentAt, Data = System.Text.Encoding.UTF8.GetString(m.Data) }).ToArray());

using(var qf = new QueueStorage(pathToQueue))
{
	qf.Initialize();
	qf.Global(actions =>
	{
		var queueNames = actions.GetAllQueuesNames();
		foreach(var name in queueNames)
		{
			var queue = actions.GetQueue(name);
			messageSelectAction(queue.GetAllMessages(null));
			foreach(var subQueue in queue.Subqueues)
			{
				messageSelectAction(queue.GetAllMessages(subQueue));
			}
		}
	});
}
messages.Dump();