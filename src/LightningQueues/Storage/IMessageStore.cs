namespace LightningQueues.Storage
{
    public interface IMessageStore
    {
        ITransaction StoreMessages(IncomingMessage[] messages);
    }
}
