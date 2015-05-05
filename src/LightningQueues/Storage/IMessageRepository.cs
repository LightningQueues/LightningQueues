namespace LightningQueues.Storage
{
    public interface IMessageRepository
    {
        IIncomingTransaction StoreMessages(IncomingMessage[] messages);
    }
}
