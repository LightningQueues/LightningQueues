using LightningDB;
using LightningQueues.Serialization;

namespace LightningQueues.Storage.LMDB;

public static class LmdbStorageExtensions
{
    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path, IMessageSerializer serializer)
    {
        return configuration.StoreMessagesWith(new LmdbMessageStore(path, serializer));
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path, 
        EnvironmentConfiguration config, IMessageSerializer serializer)
    {
        return configuration.StoreMessagesWith(new LmdbMessageStore(path, config, serializer));
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, LightningEnvironment environment,
        IMessageSerializer serializer)
    {
        return configuration.StoreMessagesWith(new LmdbMessageStore(environment, serializer));
    }
}