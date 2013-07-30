using System;
using Common.Logging;
using Rhino.Queues.Storage;

namespace Rhino.Queues.Internal
{
    public class QueueTransaction : ITransaction
    {
        private readonly QueueStorage queueStorage;
        private readonly Action assertNotDisposed;
        private readonly Action onComplete;
        private readonly ILog logger = LogManager.GetCurrentClassLogger();

        public QueueTransaction(QueueStorage queueStorage, Action onComplete, Action assertNotDisposed)
        {
            this.queueStorage = queueStorage;
            this.assertNotDisposed = assertNotDisposed;
            this.onComplete = onComplete;
            Id = Guid.NewGuid();
        }

        public Guid Id { get; private set; }

        public void Rollback()
        {
            try
            {
                assertNotDisposed();
                logger.DebugFormat("Rolling back transaction with id: {0}", Id);
                queueStorage.Global(actions =>
                {
                    actions.ReverseAllFrom(Id);
                    actions.DeleteMessageToSend(Id);
                    actions.Commit();
                });
                logger.DebugFormat("Rolledback transaction with id: {0}", Id);
            }
            catch (Exception e)
			{
				logger.Warn("Failed to rollback transaction " + Id, e);
			    throw;
			}
            finally
            {
                onComplete();
            }
        }

        public void Commit()
        {
            try
            {
                ActualCommit();
            }
            catch (Exception e)
            {
                logger.Warn("Failed to commit transaction " + Id, e);
                throw;
            }
            finally
            {
                onComplete();
            }
        }

        private void ActualCommit()
        {
            assertNotDisposed();
            logger.DebugFormat("Committing transaction with id: {0}", Id);
            queueStorage.Global(actions =>
            {
                actions.RemoveReversalsMoveCompletedMessagesAndFinishSubQueueMove(Id);
                actions.MarkAsReadyToSend(Id);
                actions.DeleteRecoveryInformation(Id);
                actions.Commit();
            });
            logger.DebugFormat("Commited transaction with id: {0}", Id); 
        }
    }
}