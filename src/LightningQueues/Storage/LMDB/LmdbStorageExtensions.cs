using System;
using LightningDB;
using LightningQueues.Serialization;

namespace LightningQueues.Storage.LMDB;

public static class LmdbStorageExtensions
{
    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path, 
        EnvironmentConfiguration config, IMessageSerializer serializer)
    {
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), serializer);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, Func<LightningEnvironment> environment,
        IMessageSerializer serializer)
    {
        return configuration.StoreMessagesWith(() => new LmdbMessageStore(environment(), serializer));
    }
}