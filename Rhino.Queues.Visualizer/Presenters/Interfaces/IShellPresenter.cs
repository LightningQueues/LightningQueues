using Caliburn.MVP.Presenters;
using Rhino.Queues.Visualizer.Views.Interfaces;

namespace Rhino.Queues.Visualizer.Presenters.Interfaces
{
	public interface IShellPresenter : IPresenterManager
	{
		IShellView View { get; set; }
	}
}