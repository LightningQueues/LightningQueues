using System.Windows;

namespace Rhino.Queues.Visualizer
{
	public interface IApplication
	{
		event ExitEventHandler Exit;
		event StartupEventHandler Startup;
	}
}