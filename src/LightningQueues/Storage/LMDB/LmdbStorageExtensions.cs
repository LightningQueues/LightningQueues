using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public static class LmdbStorageExtensions
{
    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path,
        EnvironmentConfiguration config)
    {
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), null);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration, string path,
        EnvironmentConfiguration config, LmdbStorageOptions? storageOptions)
    {
        return configuration.StoreWithLmdb(() => new LightningEnvironment(path, config), storageOptions);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration,
        Func<LightningEnvironment> environment)
    {
        return configuration.StoreWithLmdb(environment, null);
    }

    public static QueueConfiguration StoreWithLmdb(this QueueConfiguration configuration,
        Func<LightningEnvironment> environment, LmdbStorageOptions? storageOptions)
    {
        return configuration.StoreMessagesWith(() => new LmdbMessageStore(
            environment(),
            configuration.Serializer ?? throw new InvalidOperationException(
                "Serializer must be configured before storage. Call SerializeWith() first."),
            storageOptions));
    }
}