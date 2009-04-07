using System.Collections.Generic;
using System.Collections.ObjectModel;
using Caliburn.MVP.Models;

namespace Rhino.Queues.Visualizer.Model
{
	public class QueueModel : ModelBase
	{
		public QueueModel()
		{
			Queues = new ObservableCollection<QueueModel>();
		}

		private static readonly IPropertyDefinition<string> NameProperty =
			Property<QueueModel, string>(x => x.Name);

		public string Name
		{
			get { return GetValue(NameProperty); }
			set { SetValue(NameProperty, value); }
		}

		private static readonly IPropertyDefinition<string> PathProperty =
			Property<QueueModel, string>(x => x.Path);

		public string Path
		{
			get { return GetValue(PathProperty); }
			set { SetValue(PathProperty, value); }
		}

		private static readonly IPropertyDefinition<QueueModel> ParentQueueProperty =
			Property<QueueModel, QueueModel>(x => x.ParentQueueModel);

		public QueueModel ParentQueueModel
		{
			get { return GetValue(ParentQueueProperty); }
			set { SetValue(ParentQueueProperty, value); }
		}

		private static readonly IPropertyDefinition<IList<QueueModel>> SubQueuesProperty =
			Property<QueueModel, IList<QueueModel>>(x => x.Queues);

		public IList<QueueModel> Queues
		{
			get { return GetValue(SubQueuesProperty); }
			set { SetValue(SubQueuesProperty, value); }
		}

		public void AddSubqueue(QueueModel queueModel)
		{
			Queues.Add(queueModel);
			queueModel.ParentQueueModel = this;
		}
	}
}