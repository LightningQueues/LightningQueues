using System;
using FubuCore.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Internal
{
    public class QueueTransaction : ITransaction
    {
        private readonly ILogger _logger;
        private readonly QueueStorage _queueStorage;
        private readonly Action _assertNotDisposed;
        private readonly Action _onComplete;

        public QueueTransaction(ILogger logger, QueueStorage queueStorage, Action onComplete, Action assertNotDisposed)
        {
            _logger = logger;
            _queueStorage = queueStorage;
            _assertNotDisposed = assertNotDisposed;
            _onComplete = onComplete;
            Id = Guid.NewGuid();
        }

        public Guid Id { get; private set; }

        public void Rollback()
        {
            try
            {
                _assertNotDisposed();
                _logger.Debug("Rolling back transaction with id: {0}", Id);
                _queueStorage.Global(actions =>
                {
                    actions.ReverseAllFrom(Id);
                    actions.DeleteMessageToSend(Id);
                    actions.Commit();
                });
                _logger.Debug("Rolledback transaction with id: {0}", Id);
            }
            catch (Exception e)
			{
				_logger.Info("Failed to rollback transaction {0} {1}", Id, e);
			    throw;
			}
            finally
            {
                _onComplete();
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
                _logger.Info("Failed to commit transaction {0} {1}", Id, e);
                throw;
            }
            finally
            {
                _onComplete();
            }
        }

        private void ActualCommit()
        {
            _assertNotDisposed();
            _logger.Debug("Committing transaction with id: {0}", Id);
            _queueStorage.Global(actions =>
            {
                actions.RemoveReversalsMoveCompletedMessagesAndFinishSubQueueMove(Id);
                actions.MarkAsReadyToSend(Id);
                actions.DeleteRecoveryInformation(Id);
                actions.Commit();
            });
            _logger.Debug("Commited transaction with id: {0}", Id); 
        }
    }
}