using System.Collections.Generic;
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
    /// Creates options with LightningDB's built-in GuidComparer optimized for 16-byte GUID keys.
    /// This is recommended when using MessageId as the database key.
    /// </summary>
    public static LmdbStorageOptions WithGuidComparer() => new()
    {
        KeyComparer = GuidComparer.Instance
    };
}
