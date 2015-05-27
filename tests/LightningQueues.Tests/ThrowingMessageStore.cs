using LightningQueues.Storage;
using System;

namespace LightningQueues.Tests
{
    public class ThrowingMessageStore<TException> : IMessageStore
        where TException : Exception, new()
    {
        public ITransaction StoreMessages(IncomingMessage[] messages)
        {
            throw new TException();
        }
    }
}
