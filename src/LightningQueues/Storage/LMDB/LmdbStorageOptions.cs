using System;
using System.Collections.Generic;
using System.Threading;
using LightningDB;
using LightningDB.Comparers;

namespace LightningQueues.Storage.LMDB;

/// <summary>
/// Configuration options for LMDB storage including custom comparer support.
/// </summary>
public class LmdbStorageOptions
{
    /// <summary>
    /// Gets or sets the custom key comparer for database operations.
    /// If null, the default LMDB comparison (lexicographic byte order) is used.
    /// </summary>
    public IComparer<MDBValue>? KeyComparer { get; set; }

    /// <summary>
    /// Gets or sets the environment open flags.
    /// Default is NoLock | MapAsync for balanced performance and safety.
    /// </summary>
    /// <remarks>
    /// Performance flags (use with caution):
    /// - NoSync: Skip fsync on commit. Much faster but data may be lost on system crash.
    /// - WriteMap: Use writable memory map. Faster writes but risks corruption from bugs.
    /// - NoMetaSync: Don't sync metadata pages. Moderate speedup with less risk than NoSync.
    /// </remarks>
    public EnvironmentOpenFlags EnvironmentFlags { get; set; } =
        EnvironmentOpenFlags.NoLock | EnvironmentOpenFlags.MapAsync;

    /// <summary>
    /// Gets or sets whether to use LMDB's AppendData optimization for incoming message storage.
    /// When enabled, uses MDB_APPEND which is ~2x faster but requires keys to be in ascending order.
    /// This works with the timestamp-first COMB GUID format in MessageId.
    /// Default is false for compatibility; set to true for maximum write throughput.
    /// </summary>
    /// <remarks>
    /// Requirements for AppendData:
    /// - Messages must arrive in ascending MessageId order (timestamp-first COMB format ensures this)
    /// - Single-threaded writes per queue (typical for receiver scenarios)
    /// - Database should be empty or last key should be less than new keys
    /// </remarks>
    public bool UseAppendData { get; set; }

    /// <summary>
    /// Creates options with LightningDB's built-in GuidComparer optimized for 16-byte GUID keys.
    /// This is recommended when using MessageId as the database key.
    /// </summary>
    public static LmdbStorageOptions WithGuidComparer() => new()
    {
        KeyComparer = GuidComparer.Instance
    };

    /// <summary>
    /// Creates options optimized for maximum write throughput.
    /// WARNING: Sacrifices durability - data may be lost on system crash.
    /// Use only when performance is critical and data loss is acceptable.
    /// </summary>
    public static LmdbStorageOptions HighPerformance() => new()
    {
        EnvironmentFlags = EnvironmentOpenFlags.NoLock |
                          EnvironmentOpenFlags.MapAsync |
                          EnvironmentOpenFlags.NoSync |
                          EnvironmentOpenFlags.WriteMap
    };

    /// <summary>
    /// Creates options with NoMetaSync - skips metadata sync on commit.
    /// Safest performance option: maintains database integrity, may lose
    /// only the last committed transaction on OS crash.
    /// </summary>
    public static LmdbStorageOptions NoMetaSyncOnly() => new()
    {
        EnvironmentFlags = EnvironmentOpenFlags.NoLock |
                          EnvironmentOpenFlags.MapAsync |
                          EnvironmentOpenFlags.NoMetaSync
    };

    /// <summary>
    /// Creates options with NoSync + WriteMap for use with periodic manual sync.
    /// Call LightningEnvironment.Flush() periodically (e.g., every second) to
    /// ensure durability while maintaining high throughput.
    /// Risk: May lose data written since last Flush() call on crash.
    /// </summary>
    public static LmdbStorageOptions NoSyncWithPeriodicFlush() => new()
    {
        EnvironmentFlags = EnvironmentOpenFlags.NoLock |
                          EnvironmentOpenFlags.MapAsync |
                          EnvironmentOpenFlags.NoSync |
                          EnvironmentOpenFlags.WriteMap
    };

    /// <summary>
    /// Creates options for maximum write throughput using NoSync + AppendData.
    /// Combines NoSync (skip fsync) with AppendData (optimized B+ tree inserts).
    /// WARNING: Data may be lost on system crash. Use when throughput is critical.
    /// </summary>
    public static LmdbStorageOptions MaxThroughput() => new()
    {
        EnvironmentFlags = EnvironmentOpenFlags.NoLock |
                          EnvironmentOpenFlags.MapAsync |
                          EnvironmentOpenFlags.NoSync |
                          EnvironmentOpenFlags.WriteMap,
        UseAppendData = true
    };

    /// <summary>
    /// Creates options with AppendData only (safe durability, optimized inserts).
    /// Uses AppendData for faster inserts while maintaining full durability.
    /// Best for scenarios where messages arrive in ascending key order.
    /// </summary>
    public static LmdbStorageOptions WithAppendData() => new()
    {
        UseAppendData = true
    };
}

/// <summary>
/// Generates sequential 16-byte keys optimized for LMDB's MDB_APPEND mode.
/// Keys are ordered by: timestamp (8 bytes) + counter (4 bytes) + instance (4 bytes)
/// </summary>
public static class SequentialKeyGenerator
{
    private static long _counter;
    private static readonly byte[] InstanceBytes;

    static SequentialKeyGenerator()
    {
        // Use hash of machine name + process ID for instance uniqueness
        var instanceId = Environment.MachineName.GetHashCode() ^ Environment.ProcessId;
        InstanceBytes = BitConverter.GetBytes(instanceId);
    }

    /// <summary>
    /// Generates a 16-byte sequential key suitable for MDB_APPEND.
    /// Format: [8 bytes timestamp][4 bytes counter][4 bytes instance]
    /// </summary>
    public static void GenerateKey(Span<byte> destination)
    {
        if (destination.Length < 16)
            throw new ArgumentException("Destination must be at least 16 bytes", nameof(destination));

        // Timestamp in ticks (big-endian for correct sort order)
        var ticks = DateTime.UtcNow.Ticks;
        destination[0] = (byte)(ticks >> 56);
        destination[1] = (byte)(ticks >> 48);
        destination[2] = (byte)(ticks >> 40);
        destination[3] = (byte)(ticks >> 32);
        destination[4] = (byte)(ticks >> 24);
        destination[5] = (byte)(ticks >> 16);
        destination[6] = (byte)(ticks >> 8);
        destination[7] = (byte)ticks;

        // Counter (big-endian for correct sort order)
        var count = (uint)Interlocked.Increment(ref _counter);
        destination[8] = (byte)(count >> 24);
        destination[9] = (byte)(count >> 16);
        destination[10] = (byte)(count >> 8);
        destination[11] = (byte)count;

        // Instance ID
        InstanceBytes.CopyTo(destination.Slice(12, 4));
    }
}
