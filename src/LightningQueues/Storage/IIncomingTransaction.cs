namespace LightningQueues.Storage
{
    public interface IIncomingTransaction
    {
        void Commit();
        void Rollback();
    }
}
