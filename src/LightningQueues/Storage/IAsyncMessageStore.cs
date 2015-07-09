using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface IAsyncMessageStore
    {
        Task<IAsyncTransaction> BeginTransaction();
        Task StoreMessages(IAsyncTransaction transaction, params IncomingMessage[] messages);
        Task MoveToQueue(IAsyncTransaction transaction, string queueName, IncomingMessage message);
    }
}