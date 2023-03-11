namespace LightningQueues.Storage;

public interface ITransaction
{
    void Commit();
    void Rollback();
}