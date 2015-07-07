using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface IMessageStore
    {
        Task<ITransaction> StoreMessages(IncomingMessage[] messages);
    }
}
