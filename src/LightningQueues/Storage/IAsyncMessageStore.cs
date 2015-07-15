using System.Collections.Generic;
using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface IAsyncMessageStore
    {
        Task<IAsyncTransaction> BeginTransaction();
        Task StoreMessages(IAsyncTransaction transaction, params Message[] messages);
        Task MoveToQueue(IAsyncTransaction transaction, string queueName, Message message);
        Task FailedToSend(IAsyncTransaction transaction, params OutgoingMessage[] messages);
        Task SuccessfullySent(IAsyncTransaction transaction, params OutgoingMessage[] messages);
    }
}