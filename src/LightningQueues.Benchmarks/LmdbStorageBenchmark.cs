using BenchmarkDotNet.Attributes;
using LightningDB;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Benchmarks;

/// <summary>
/// Benchmark comparing LMDB storage operations:
/// - Default vs GuidComparer key comparison
/// - Full serialization path vs zero-copy raw bytes
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

    // Zero-copy baseline data: pre-serialized bytes for direct LMDB Put
    private byte[][]? _preSerializedMessages;
    private byte[][]? _messageIdBytes;

    // Sequential key data for AppendData benchmark
    private byte[][]? _sequentialKeys;
    private LightningEnvironment? _environmentNoSync;
    private string _pathNoSync = null!;

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
        _preSerializedMessages = new byte[MessageCount][];
        _messageIdBytes = new byte[MessageCount][];
        var random = new Random(42); // Fixed seed for reproducibility

        for (var i = 0; i < MessageCount; i++)
        {
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            _messages[i] = Message.Create(data: data, queue: "test");
            _messageIds[i] = _messages[i].Id;

            // Pre-serialize for zero-copy benchmark
            _preSerializedMessages[i] = serializer.AsSpan(_messages[i]).ToArray();

            // Pre-extract MessageId bytes for zero-copy benchmark
            _messageIdBytes[i] = new byte[16];
            _messages[i].Id.MessageIdentifier.TryWriteBytes(_messageIdBytes[i]);
        }

        // Generate sequential keys for AppendData benchmark
        _sequentialKeys = new byte[MessageCount][];
        for (var i = 0; i < MessageCount; i++)
        {
            _sequentialKeys[i] = new byte[16];
            SequentialKeyGenerator.GenerateKey(_sequentialKeys[i]);
        }

        // NoSync environment for maximum performance benchmarks
        _pathNoSync = Path.Combine(basePath, "nosync");
        Directory.CreateDirectory(_pathNoSync);
        _environmentNoSync = new LightningEnvironment(_pathNoSync, envConfig);
        _environmentNoSync.Open(EnvironmentOpenFlags.NoLock | EnvironmentOpenFlags.MapAsync |
                                EnvironmentOpenFlags.NoSync | EnvironmentOpenFlags.WriteMap);
        using (var tx = _environmentNoSync.BeginTransaction())
        {
            tx.OpenDatabase("test", new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });
            tx.Commit();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _storeDefault?.Dispose();
        _storeGuidComparer?.Dispose();
        _environmentNoSync?.Dispose();

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

        // Clear NoSync environment
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        tx.TruncateDatabase(db);
        tx.Commit();

        // Regenerate sequential keys (must be fresh for AppendData to work)
        for (var i = 0; i < MessageCount; i++)
        {
            SequentialKeyGenerator.GenerateKey(_sequentialKeys![i]);
        }
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

    // --- Zero-Copy Baseline Operations ---
    // These benchmarks store pre-serialized bytes directly to LMDB,
    // eliminating all serialization overhead. This is the TRUE target
    // for network stack performance.

    [Benchmark]
    public void PutRawBytes_ZeroCopy()
    {
        // Store pre-serialized bytes directly - no serialization during benchmark
        using var tx = _environmentDefault!.BeginTransaction();
        using var db = tx.OpenDatabase("test");

        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _messageIdBytes![i], _preSerializedMessages![i]);
        }

        tx.Commit();
    }

    [Benchmark]
    public int GetRawBytes_ZeroCopy()
    {
        // First store the data
        using (var tx = _environmentDefault!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                tx.Put(db, _messageIdBytes![i], _preSerializedMessages![i]);
            }
            tx.Commit();
        }

        // Then retrieve raw bytes - no deserialization
        var count = 0;
        using (var tx = _environmentDefault.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                var result = tx.Get(db, _messageIdBytes![i]);
                if (result.resultCode == MDBResultCode.Success)
                    count++;
            }
        }
        return count;
    }

    [Benchmark]
    public int EnumerateRawBytes_ZeroCopy()
    {
        // First store the data
        using (var tx = _environmentDefault!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                tx.Put(db, _messageIdBytes![i], _preSerializedMessages![i]);
            }
            tx.Commit();
        }

        // Enumerate raw bytes - no deserialization
        var count = 0;
        using (var tx = _environmentDefault.BeginTransaction(TransactionBeginFlags.ReadOnly))
        {
            using var db = tx.OpenDatabase("test");
            using var cursor = tx.CreateCursor(db);
            foreach (var _ in cursor.AsEnumerable())
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark]
    public void DeleteRawBytes_ZeroCopy()
    {
        // First store the data
        using (var tx = _environmentDefault!.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                tx.Put(db, _messageIdBytes![i], _preSerializedMessages![i]);
            }
            tx.Commit();
        }

        // Delete by key - no deserialization needed
        using (var tx = _environmentDefault.BeginTransaction())
        {
            using var db = tx.OpenDatabase("test");
            for (var i = 0; i < MessageCount; i++)
            {
                tx.Delete(db, _messageIdBytes![i]);
            }
            tx.Commit();
        }
    }

    // --- NoSync + Sequential Key Benchmarks ---

    /// <summary>
    /// Random keys with NoSync - baseline for comparison.
    /// </summary>
    [Benchmark]
    public void PutRawBytes_NoSync_RandomKeys()
    {
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _messageIdBytes![i], _preSerializedMessages![i]);
        }
        tx.Commit();
    }

    /// <summary>
    /// Sequential keys with NoSync - should be faster due to better B+ tree locality.
    /// </summary>
    [Benchmark]
    public void PutRawBytes_NoSync_SequentialKeys()
    {
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _sequentialKeys![i], _preSerializedMessages![i]);
        }
        tx.Commit();
    }

    /// <summary>
    /// Sequential keys with NoSync + AppendData flag - maximum insert performance.
    /// Keys MUST be inserted in ascending order for AppendData to work.
    /// </summary>
    [Benchmark]
    public void PutRawBytes_NoSync_SequentialKeys_Append()
    {
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        for (var i = 0; i < MessageCount; i++)
        {
            tx.Put(db, _sequentialKeys![i], _preSerializedMessages![i], PutOptions.AppendData);
        }
        tx.Commit();
    }

    // --- Fresh COMB GUID Benchmarks ---
    // These test the new timestamp-first COMB format which should enable AppendData

    /// <summary>
    /// Fresh COMB GUIDs generated during iteration - tests the new timestamp-first format.
    /// COMBs now have timestamp at bytes 0-5 with counter at bytes 6-7 for strict ordering.
    /// </summary>
    [Benchmark]
    public void PutRawBytes_NoSync_FreshComb()
    {
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        Span<byte> keyBuffer = stackalloc byte[16];

        for (var i = 0; i < MessageCount; i++)
        {
            var id = MessageId.GenerateRandom();
            id.MessageIdentifier.TryWriteBytes(keyBuffer);
            tx.Put(db, keyBuffer, _preSerializedMessages![i]);
        }
        tx.Commit();
    }

    /// <summary>
    /// Fresh COMB GUIDs with AppendData - should work with new timestamp-first format.
    /// This is the target: COMB keys that work with AppendData for 2x faster inserts.
    /// </summary>
    [Benchmark]
    public void PutRawBytes_NoSync_FreshComb_Append()
    {
        using var tx = _environmentNoSync!.BeginTransaction();
        using var db = tx.OpenDatabase("test");
        Span<byte> keyBuffer = stackalloc byte[16];

        for (var i = 0; i < MessageCount; i++)
        {
            var id = MessageId.GenerateRandom();
            id.MessageIdentifier.TryWriteBytes(keyBuffer);
            tx.Put(db, keyBuffer, _preSerializedMessages![i], PutOptions.AppendData);
        }
        tx.Commit();
    }
}
