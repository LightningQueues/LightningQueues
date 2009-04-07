using Caliburn.MVP.Presenters;

namespace Rhino.Queues.Visualizer
{
	public interface IApplicationController
	{
		void Initialize();
		void Open<T>() where T : IPresenter;
		void Open(IPresenter presenter);
	}
}