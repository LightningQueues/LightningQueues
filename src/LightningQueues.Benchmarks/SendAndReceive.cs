using BenchmarkDotNet.Attributes;
using LightningDB;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Benchmarks;

/// <summary>
/// Storage configuration options for benchmarking
/// </summary>
public enum StorageMode
{
    /// <summary>Default: MapAsync + NoLock</summary>
    Default,
    /// <summary>AppendData enabled for faster sequential inserts</summary>
    AppendData,
    /// <summary>NoSync + WriteMap for maximum throughput (data loss risk on crash)</summary>
    NoSync,
    /// <summary>NoSync + WriteMap + AppendData - maximum performance</summary>
    MaxThroughput
}

[MemoryDiagnoser]
public class SendAndReceive
{
    private Queue? _sender;
    private Queue? _receiver;
    private Message[]? _messages;
    private Task? _receivingTask;

    [Params(100)]
    public int MessageCount { get; set; }

    [Params(64)]
    public int MessageDataSize { get; set; }

    [Params(StorageMode.Default, StorageMode.AppendData, StorageMode.NoSync, StorageMode.MaxThroughput)]
    public StorageMode Mode { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var senderPath = Path.Combine(Path.GetTempPath(), "sender", Guid.NewGuid().ToString());
        var receiverPath = Path.Combine(Path.GetTempPath(), "receiver", Guid.NewGuid().ToString());
        _messages = new Message[MessageCount];
        var envConfig = new EnvironmentConfiguration { MapSize = 1024 * 1024 * 100, MaxDatabases = 5 };

        var storageOptions = Mode switch
        {
            StorageMode.Default => null,
            StorageMode.AppendData => LmdbStorageOptions.WithAppendData(),
            StorageMode.NoSync => LmdbStorageOptions.HighPerformance(),
            StorageMode.MaxThroughput => LmdbStorageOptions.MaxThroughput(),
            _ => null
        };

        _sender = new QueueConfiguration()
            .WithDefaults()
            .StoreWithLmdb(senderPath, envConfig, storageOptions)
            .BuildQueue();
        _sender.CreateQueue("sender");
        _receiver = new QueueConfiguration()
            .WithDefaults()
            .StoreWithLmdb(receiverPath, envConfig, storageOptions)
            .BuildQueue();
        _sender.CreateQueue("receiver");
        _sender.Start();
        _receiver.Start();
        _receivingTask = Task.Factory.StartNew(async () =>
        {
            var count = 0;
            await foreach (var _ in _receiver.Receive("receiver"))
            {
                Interlocked.Increment(ref count);
                if (count == MessageCount)
                    break;
            }
        });
        var random = new Random();
        for (var i = 0; i < MessageCount; ++i)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            var msg = Message.Create(
                data: data,
                queue: "receiver",
                destinationUri: $"lq.tcp://{_receiver.Endpoint}"
            );
            _messages[i] = msg;
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (_sender != null && _receiver != null)
        {
            using(_sender)
            using (_receiver)
            {
            }
        }
    }

    [Benchmark]
    public async ValueTask Run()
    {
        for (var i = 0; i < MessageCount; ++i)
        {
            _sender?.Send(_messages![i]);
        }

        if(_receivingTask != null)
            await _receivingTask;
    }
}