using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Caliburn.MVP.Presenters;
using Rhino.Queues.Visualizer.Presenters.Interfaces;
using Caliburn.Core.Metadata;
using Rhino.Queues.Visualizer.Framework;
using Rhino.Queues.Visualizer.Services;
using Rhino.Queues.Visualizer.Views;
using Rhino.Queues.Visualizer.Model;
using Caliburn.Actions.Filters;
using Caliburn.Actions;
using System;

namespace Rhino.Queues.Visualizer.Presenters
{
	[View(typeof(QueueView))]
	[PerRequest(typeof(IQueuePresenter))]
	public class QueuePresenter : Presenter, IQueuePresenter
	{
		private readonly IQueueRepository queueRepository;
		private readonly IMessageRepository messageRepository;

		public QueuePresenter(IQueueRepository queueRepository, IMessageRepository messageRepository)
		{
			this.queueRepository = queueRepository;
			this.messageRepository = messageRepository;
			Queues = new ObservableCollection<QueueModel>();
		}

		private string errorMessage;

		public string ErrorMessage
		{
			get { return errorMessage; }
			set
			{
				errorMessage = value;
				NotifyOfPropertyChange("ErrorMessage");
			}
		}

		public IList<QueueModel> Queues { get; private set; }

		[AsyncAction(Callback = "AddQueue", BlockInteraction = true)]
		[Preview("GetMessagesPreview", AffectsTriggers = true)]
		[Rescue("Rescue")]
		public QueueModel GetQueueWithSubQueues(string path, string queueName)
		{
			if(Queues.Any(q => q.Name == queueName))
				return null;
			ErrorMessage = null;
			return queueRepository.Get(path, queueName);
		}

		public void AddQueue(QueueModel queue)
		{
			if(queue == null)
				return;
			Queues.Add(queue);
		}

		[AsyncAction]
		[Rescue("Rescue")]
		public IList<MessageModel> GetMessages(QueueModel queue)
		{
			ErrorMessage = null;
			return messageRepository.GetMessages(queue);
		}

		public void Rescue(Exception ex)
		{
			ErrorMessage = string.Format("{0}: {1}", ex.GetType().Name, ex.Message);
		}

		public bool GetMessagesPreview(string path, string queueName)
		{
			return !string.IsNullOrEmpty(path)
				   && !string.IsNullOrEmpty(queueName);
		}

		
	}
}