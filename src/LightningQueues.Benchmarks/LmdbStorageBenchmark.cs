using BenchmarkDotNet.Attributes;
using LightningDB;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Benchmarks;

/// <summary>
/// Benchmark comparing LMDB storage operations with default byte comparison
/// vs the built-in GuidComparer optimized for 16-byte GUID keys.
/// </summary>
[MemoryDiagnoser]
[RankColumn]
public class LmdbStorageBenchmark
{
    private LightningEnvironment? _environmentDefault;
    private LightningEnvironment? _environmentGuidComparer;
    private LmdbMessageStore? _storeDefault;
    private LmdbMessageStore? _storeGuidComparer;
    private Message[]? _messages;
    private MessageId[]? _messageIds;
    private string _pathDefault = null!;
    private string _pathGuidComparer = null!;

    [Params(10, 100)]
    public int MessageCount { get; set; }

    [Params(64)]
    public int MessageDataSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var basePath = Path.Combine(Path.GetTempPath(), "lmdb-bench", Guid.NewGuid().ToString());
        _pathDefault = Path.Combine(basePath, "default");
        _pathGuidComparer = Path.Combine(basePath, "guid-comparer");

        Directory.CreateDirectory(_pathDefault);
        Directory.CreateDirectory(_pathGuidComparer);

        var envConfig = new EnvironmentConfiguration
        {
            MapSize = 1024 * 1024 * 500, // 500MB for larger test runs
            MaxDatabases = 5
        };

        var serializer = new MessageSerializer();

        // Default store (lexicographic byte comparison)
        _environmentDefault = new LightningEnvironment(_pathDefault, envConfig);
        _storeDefault = new LmdbMessageStore(_environmentDefault, serializer);
        _storeDefault.CreateQueue("test");

        // GuidComparer store (optimized for 16-byte GUID keys)
        _environmentGuidComparer = new LightningEnvironment(_pathGuidComparer, envConfig);
        _storeGuidComparer = new LmdbMessageStore(_environmentGuidComparer, serializer,
            LmdbStorageOptions.WithGuidComparer());
        _storeGuidComparer.CreateQueue("test");

        // Generate messages with deterministic data
        _messages = new Message[MessageCount];
        _messageIds = new MessageId[MessageCount];
        var random = new Random(42); // Fixed seed for reproducibility

        for (var i = 0; i < MessageCount; i++)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            _messages[i] = Message.Create(data: data, queue: "test");
            _messageIds[i] = _messages[i].Id;
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _storeDefault?.Dispose();
        _storeGuidComparer?.Dispose();

        try
        {
            var basePath = Path.GetDirectoryName(_pathDefault);
            if (basePath != null && Directory.Exists(basePath))
                Directory.Delete(basePath, true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Clear data between iterations for consistent measurements
        _storeDefault?.ClearAllStorage();
        _storeGuidComparer?.ClearAllStorage();
    }

    // --- Put Operations ---

    [Benchmark]
    public void PutMessages_Default()
    {
        _storeDefault!.StoreIncoming(_messages!);
    }

    [Benchmark]
    public void PutMessages_GuidComparer()
    {
        _storeGuidComparer!.StoreIncoming(_messages!);
    }

    // --- Get Operations ---

    [Benchmark]
    public int GetMessages_Default()
    {
        _storeDefault!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var id in _messageIds!)
        {
            var msg = _storeDefault.GetMessage("test", id);
            if (msg != null) count++;
        }
        return count;
    }

    [Benchmark]
    public int GetMessages_GuidComparer()
    {
        _storeGuidComparer!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var id in _messageIds!)
        {
            var msg = _storeGuidComparer.GetMessage("test", id);
            if (msg != null) count++;
        }
        return count;
    }

    // --- Enumerate Operations ---

    [Benchmark]
    public int EnumerateMessages_Default()
    {
        _storeDefault!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var _ in _storeDefault.PersistedIncoming("test"))
        {
            count++;
        }
        return count;
    }

    [Benchmark]
    public int EnumerateMessages_GuidComparer()
    {
        _storeGuidComparer!.StoreIncoming(_messages!);
        var count = 0;
        foreach (var _ in _storeGuidComparer.PersistedIncoming("test"))
        {
            count++;
        }
        return count;
    }

    // --- Delete Operations ---

    [Benchmark]
    public void DeleteMessages_Default()
    {
        _storeDefault!.StoreIncoming(_messages!);
        _storeDefault.DeleteIncoming(_messages!);
    }

    [Benchmark]
    public void DeleteMessages_GuidComparer()
    {
        _storeGuidComparer!.StoreIncoming(_messages!);
        _storeGuidComparer.DeleteIncoming(_messages!);
    }
}
