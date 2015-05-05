using LightningQueues.Storage;
using System;

namespace LightningQueues.Tests
{
    public class ThrowingMessageRepository<TException> : IMessageRepository
        where TException : Exception, new()
    {
        public IIncomingTransaction StoreMessages(IncomingMessage[] messages)
        {
            throw new TException();
        }
    }
}
