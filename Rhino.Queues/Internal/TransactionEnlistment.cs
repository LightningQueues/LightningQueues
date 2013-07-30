using System;
using System.Transactions;
using Common.Logging;
using Rhino.Queues.Storage;
using Transaction = System.Transactions.Transaction;

namespace Rhino.Queues.Internal
{
	public class TransactionEnlistment : QueueTransaction, ISinglePhaseNotification
	{
		private readonly QueueStorage queueStorage;
		private readonly Action assertNotDisposed;
		private readonly ILog logger = LogManager.GetLogger(typeof(TransactionEnlistment));

		public TransactionEnlistment(QueueStorage queueStorage, Action onComplete, Action assertNotDisposed)
            : base(queueStorage, assertNotDisposed, onComplete)
		{
			this.queueStorage = queueStorage;
			this.assertNotDisposed = assertNotDisposed;

			var transaction = Transaction.Current;
			if (transaction != null)// should happen only during recovery
			{
				transaction.EnlistDurable(queueStorage.Id,
										  this,
										  EnlistmentOptions.None);
			}
			logger.DebugFormat("Enlisting in the current transaction with enlistment id: {0}", Id);
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			assertNotDisposed();
			logger.DebugFormat("Preparing enlistment with id: {0}", Id);
			var information = preparingEnlistment.RecoveryInformation();
			queueStorage.Global(actions =>
			{
				actions.RegisterRecoveryInformation(Id, information);
				actions.Commit();
			});
			preparingEnlistment.Prepared();
			logger.DebugFormat("Prepared enlistment with id: {0}", Id);
		}

		public void Commit(Enlistment enlistment)
		{
			try
			{
                Commit();
				enlistment.Done();
			}
			catch (Exception)
			{
                //on a callback thread, can't throw
			}
		}

		public void Rollback(Enlistment enlistment)
		{
			try
			{
                Rollback();
				enlistment.Done();
			}
			catch (Exception)
			{
                //on a callback thread, can't throw
			}
		}

		public void InDoubt(Enlistment enlistment)
		{
			enlistment.Done();
		}

		public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			try
			{
                Commit();
				singlePhaseEnlistment.Done();
			}
			catch (Exception)
			{
                //on a callback thread, can't throw
			}
		}
	}
}
