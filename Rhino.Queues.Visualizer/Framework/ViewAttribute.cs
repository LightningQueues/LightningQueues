using System;

namespace Rhino.Queues.Visualizer.Framework
{
	public class ViewAttribute : Attribute
	{
		private readonly Type _viewType;

		public Type ViewType
		{
			get { return _viewType; }
		}

		public ViewAttribute(Type viewType)
		{
			_viewType = viewType;
		}
	}
}