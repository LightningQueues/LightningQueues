using System.Linq;
using System.Reflection;
using System.Windows;
using Caliburn.Actions;
using Caliburn.Castle;
using Caliburn.Core;
using Caliburn.RoutedUIMessaging;
using Castle.Core;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using Castle.Windsor.Configuration.Interpreters;
using Microsoft.Practices.ServiceLocation;
using Rhino.Queues.Visualizer.Framework;
using Rhino.Queues.Visualizer.Services;

namespace Rhino.Queues.Visualizer
{
	/// <summary>
	/// Interaction logic for App.xaml
	/// </summary>
	public partial class App : Application, IApplication
	{
		protected override void OnStartup(StartupEventArgs e)
		{
			InitializeComponent();
			log4net.Config.XmlConfigurator.Configure();
			var container = new WindsorContainer(new XmlInterpreter());
			container.Register(Component.For<IApplication>().Instance(this));
			container.Register(Component.For<IQueueManagerCache>()
				.ImplementedBy<QueueManagerCache>()
				.LifeStyle.Singleton);
			container.Register(Component.For<IQueueRepository>()
				.ImplementedBy<LocalQueueRepository>()
				.LifeStyle.Transient);
			container.Register(Component.For<IMessageRepository>()
				.ImplementedBy<LocalMessageRepository>()
				.LifeStyle.Transient);

			var adapter = new WindsorAdapter(container);
			CaliburnApplication
				.ConfigureCore(adapter)
				.WithAssemblies(Assembly.GetExecutingAssembly())
				.WithActions()
				.WithRoutedUIMessaging()
				.StartApplication();

			var views = (from a in GetType().Assembly.GetTypes()
						 let attributes = a.GetCustomAttributes(typeof(ViewAttribute), false)
						 where attributes.Any()
						 select attributes).SelectMany(at => at.Select(a => ((ViewAttribute)a).ViewType));

			container.Register(
				AllTypes.From(views)
				.Where(t => true).Configure(registration => registration.LifeStyle.Is(LifestyleType.Transient))
				);

			ServiceLocator.Current.GetInstance<IApplicationController>().Initialize();

			base.OnStartup(e);
			
		}
	}
}
