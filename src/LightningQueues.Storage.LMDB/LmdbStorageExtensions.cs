namespace LightningQueues.Storage.LMDB
{
    public static class LmdbStorageExtensions
    {
        public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path)
        {
            return configuration.StoreMessagesWith(new LmdbMessageStore(path));
        }
    }
}