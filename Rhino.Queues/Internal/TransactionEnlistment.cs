using System;
using System.Transactions;
using log4net;
using Rhino.Queues.Storage;
using Transaction=System.Transactions.Transaction;

namespace Rhino.Queues.Internal
{
    public class TransactionEnlistment : IEnlistmentNotification
    {
        private readonly QueueFactory queueFactory;
        private readonly Action onCompelete;
        private ILog logger = LogManager.GetLogger(typeof (TransactionEnlistment));

        public TransactionEnlistment(QueueFactory queueFactory, Action onCompelete)
        {
            this.queueFactory = queueFactory;
            this.onCompelete = onCompelete;

            Transaction.Current.EnlistDurable(queueFactory.Id,
                                              this,
                                              EnlistmentOptions.None);
            Id = Guid.NewGuid();
            logger.DebugFormat("Enlisting in the current transaction with enlistment id: {0}", Id);
        }

        public Guid Id
        {
            get; private set;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            logger.DebugFormat("Preparing enlistment with id: {0}", Id); 
            var information = preparingEnlistment.RecoveryInformation();
            queueFactory.Global(actions =>
            {
                actions.RegisterRecoveryInformation(Id, information);
                actions.Commit();
            });
            preparingEnlistment.Prepared();
            logger.DebugFormat("Prepared enlistment with id: {0}", Id);
        }

        public void Commit(Enlistment enlistment)
        {
            logger.DebugFormat("Committing enlistment with id: {0}", Id);
            queueFactory.Global(actions =>
            {
                actions.RemoveReversalsMoveCompletedMessagesAndFinishSubQueueMove(Id);
                actions.MarkAsReadyToSend(Id);
                actions.DeleteRecoveryInformation(Id);
                actions.Commit();
            });
            enlistment.Done();
            logger.DebugFormat("Commited enlistment with id: {0}", Id);
            onCompelete();
        }

        public void Rollback(Enlistment enlistment)
        {
            logger.DebugFormat("Rolling back enlistment with id: {0}", Id);
            queueFactory.Global(actions =>
            {
                actions.ReverseAllFrom(Id);
                actions.DeleteMessageToSend(Id);
                actions.Commit();
            });
            enlistment.Done();
            logger.DebugFormat("Rolledback enlistment with id: {0}", Id);
            onCompelete();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }
    }
}