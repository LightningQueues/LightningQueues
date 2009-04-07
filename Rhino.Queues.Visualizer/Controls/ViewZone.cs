using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Caliburn.Actions;
using Microsoft.Practices.ServiceLocation;
using Rhino.Queues.Visualizer.Framework;

namespace Rhino.Queues.Visualizer.Controls
{
	public class ViewZone : ContentControl
	{
		public static readonly DependencyProperty PresenterProperty =
			DependencyProperty.Register(
				"Presenter",
				typeof(object),
				typeof(ViewZone),
				new PropertyMetadata(null, PresenterChanged)
				);

		public object Presenter
		{
			get { return GetValue(PresenterProperty); }
			set { SetValue(PresenterProperty, value); }
		}

		private static void PresenterChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			if (e.NewValue != e.OldValue && e.NewValue != null)
			{
				var atts = e.NewValue.GetType().GetCustomAttributes(typeof(ViewAttribute), true).OfType<ViewAttribute>().ToList();

				if (atts.Count < 1) return;

				var view = ServiceLocator.Current.GetInstance(atts[0].ViewType);

				Action.SetTarget(view as DependencyObject, e.NewValue);

				((ContentControl)d).Content = view;
			}
			else if (e.NewValue == null)
			{
				((ContentControl)d).Content = null;
			}
		}
	}
}