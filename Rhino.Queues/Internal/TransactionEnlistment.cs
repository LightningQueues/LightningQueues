using System;
using System.Transactions;
using Rhino.Queues.Storage;
using Transaction=System.Transactions.Transaction;

namespace Rhino.Queues.Internal
{
    public class TransactionEnlistment : IEnlistmentNotification
    {
        private readonly QueueFactory queueFactory;
        private readonly Action onCompelete;

        public TransactionEnlistment(QueueFactory queueFactory, Action onCompelete)
        {
            this.queueFactory = queueFactory;
            this.onCompelete = onCompelete;

            Transaction.Current.EnlistDurable(queueFactory.Id,
                                              this,
                                              EnlistmentOptions.None);
            Id = Guid.NewGuid();
        }

        public Guid Id
        {
            get; set;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            var information = preparingEnlistment.RecoveryInformation();
            queueFactory.Global(actions =>
            {
                actions.RegisterRecoveryInformation(Id, information);
                actions.Commit();
            });

            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            queueFactory.Global(actions =>
            {
                actions.RemoveReversalsMoveCompletedMessagesAndFinishSubQueueMove(Id);
                actions.MarkAsReadyToSend(Id);
                actions.DeleteRecoveryInformation(Id);
                actions.Commit();
            });
            enlistment.Done();
            onCompelete();
        }

        public void Rollback(Enlistment enlistment)
        {
            queueFactory.Global(actions =>
            {
                actions.ReverseAllFrom(Id);
                actions.DeleteMessageToSend(Id);
                actions.Commit();
            });
            enlistment.Done();
            onCompelete();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }
    }
}