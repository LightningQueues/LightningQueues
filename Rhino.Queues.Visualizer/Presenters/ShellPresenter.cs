using System.Windows;
using Caliburn.Actions;
using Caliburn.Core;
using Caliburn.Core.Metadata;
using Caliburn.MVP.Presenters;
using Rhino.Queues.Visualizer.Presenters.Interfaces;
using Rhino.Queues.Visualizer.Views.Interfaces;

namespace Rhino.Queues.Visualizer.Presenters
{
	[Singleton(typeof(IShellPresenter))]
	public class ShellPresenter : PresenterManager, IShellPresenter
	{
		public ShellPresenter(IShellView view)
		{
			View = view;
		}

		public IShellView View
		{
			get { return this.GetView<IShellView>(); }
			set
			{
				var target = value as DependencyObject;
				if (target != null) Action.SetTarget(target, this);
			}
		}
	}
}