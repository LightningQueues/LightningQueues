using System;
using System.Transactions;
using FubuCore.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Internal
{
	public class TransactionEnlistment : QueueTransaction, ISinglePhaseNotification
	{
	    private readonly ILogger _logger;
	    private readonly QueueStorage _queueStorage;
		private readonly Action _assertNotDisposed;

		public TransactionEnlistment(ILogger logger, QueueStorage queueStorage, Action onComplete, Action assertNotDisposed)
            : base(logger, queueStorage, assertNotDisposed, onComplete)
		{
		    _logger = logger;
		    _queueStorage = queueStorage;
			_assertNotDisposed = assertNotDisposed;

			var transaction = Transaction.Current;
			if (transaction != null)// should happen only during recovery
			{
				transaction.EnlistDurable(queueStorage.Id,
										  this,
										  EnlistmentOptions.None);
			}
			_logger.Debug("Enlisting in the current transaction with enlistment id: {0}", Id);
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			_assertNotDisposed();
			_logger.Debug("Preparing enlistment with id: {0}", Id);
			var information = preparingEnlistment.RecoveryInformation();
			_queueStorage.Global(actions =>
			{
				actions.RegisterRecoveryInformation(Id, information);
				actions.Commit();
			});
			preparingEnlistment.Prepared();
			_logger.Debug("Prepared enlistment with id: {0}", Id);
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
