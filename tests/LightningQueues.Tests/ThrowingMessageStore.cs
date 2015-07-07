using LightningQueues.Storage;
using System;
using System.Threading.Tasks;

namespace LightningQueues.Tests
{
    public class ThrowingMessageStore<TException> : IMessageStore
        where TException : Exception, new()
    {
        public Task<ITransaction> StoreMessages(IncomingMessage[] messages)
        {
            throw new TException();
        }
    }
}
