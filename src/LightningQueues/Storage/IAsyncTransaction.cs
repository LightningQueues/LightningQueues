using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface IAsyncTransaction
    {
        Task Commit();
        Task Rollback();
    }
}