namespace Rhino.Queues.Utils
{
	using System;
	using System.Collections.Generic;
	using System.Threading;
	using Model;

	public class ThreadSafeSet<T>
	{
		private readonly HashSet<T> inner = new HashSet<T>();
		private readonly ReaderWriterLockSlim rwl = new ReaderWriterLockSlim();

		public void Add(IEnumerable<T> items)
		{
			rwl.EnterWriteLock();
			try
			{
				foreach (var item in items)
				{
					inner.Add(item);
				}
			}
			finally
			{
				rwl.ExitWriteLock();
			}
		}

		public IEnumerable<TK> Filter<TK>(IEnumerable<TK> items, Func<TK,T> translator)
		{
			rwl.EnterReadLock();
			try
			{
				foreach (var item in items)
				{
					if (inner.Contains(translator(item)))
						continue;
					yield return item;
				}
			}
			finally
			{
				rwl.ExitReadLock();
			}
		}

		public void Remove(IEnumerable<T> items)
		{
			rwl.EnterWriteLock();
			try
			{
				foreach (var item in items)
				{
					inner.Remove(item);
				}
			}
			finally
			{
				rwl.ExitWriteLock();
			}
			
		}
	}
}