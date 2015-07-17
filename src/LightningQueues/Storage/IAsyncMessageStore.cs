using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface IAsyncMessageStore
    {
        Task<IAsyncTransaction> BeginTransaction();
        Task StoreMessages(IAsyncTransaction transaction, params Message[] messages);
    }
}