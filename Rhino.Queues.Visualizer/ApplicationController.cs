using System.Windows;
using Caliburn.Core.Metadata;
using Caliburn.MVP.Presenters;
using Microsoft.Practices.ServiceLocation;
using Rhino.Queues.Visualizer.Presenters.Interfaces;

namespace Rhino.Queues.Visualizer
{
	[Singleton(typeof(IApplicationController))]
	public class ApplicationController : IApplicationController
	{
		private readonly IServiceLocator serviceLocator;
		private readonly IApplication application;
		private IShellPresenter shellPresenter;

		public ApplicationController(IApplication application, IServiceLocator serviceLocator)
		{
			this.serviceLocator = serviceLocator;
			this.application = application;
		}

		public void Initialize()
		{
			application.Exit += OnExit;
			application.Startup += OnStartup;
		}

		public void Open<T>() where T : IPresenter
		{
			var presenter = serviceLocator.GetInstance<T>();
			Open(presenter);
		}

		public void Open(IPresenter presenter)
		{
			shellPresenter.Open(presenter);
		}

		private void OnStartup(object sender, StartupEventArgs e)
		{
			shellPresenter = serviceLocator.GetInstance<IShellPresenter>();
			shellPresenter.View.Show();
			Open<IQueuePresenter>();
		}

		private void OnExit(object sender, ExitEventArgs e)
		{
			
		}
	}
}