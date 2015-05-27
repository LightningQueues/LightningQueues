namespace LightningQueues.Storage
{
    public class NoPersistenceMessageStore : IMessageStore
    {
        public ITransaction StoreMessages(IncomingMessage[] messages)
        {
            return new NulloTransaction();
        }
    }
}
