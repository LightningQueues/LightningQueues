namespace LightningQueues.Storage
{
    public class NoPersistenceMessageRepository : IMessageRepository
    {
        public IIncomingTransaction StoreMessages(IncomingMessage[] messages)
        {
            return new NulloIncomingTransaction();
        }
    }
}
