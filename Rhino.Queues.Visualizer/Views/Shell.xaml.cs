using System.Windows;
using Caliburn.Core.Metadata;
using Rhino.Queues.Visualizer.Views.Interfaces;

namespace Rhino.Queues.Visualizer.Views
{
	/// <summary>
	/// Interaction logic for Shell.xaml
	/// </summary>
	[Singleton(typeof(IShellView))]
	public partial class Shell : Window, IShellView
	{
		public Shell()
		{
			InitializeComponent();
		}
	}
}
