using System;
using System.Threading.Tasks;

namespace LightningQueues.Storage
{
    public interface ITransaction
    {
        Guid TransactionId { get; }
        Task Commit();
        Task Rollback();
    }
}
