using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace LightningQueues;

/// <summary>
/// Represents a unique identifier for messages in the LightningQueues system.
/// </summary>
/// <remarks>
/// MessageId combines a source instance identifier and a message-specific identifier
/// to ensure global uniqueness. The identifiers use a COMB (Combined GUID) approach
/// which embeds timestamp information at the BEGINNING of the GUID bytes.
/// This timestamp-first ordering enables LMDB's MDB_APPEND optimization for
/// sequential key inserts, providing ~2x faster storage performance.
/// </remarks>
public record MessageId
{
    private static readonly Guid InstanceId = GenerateGuidComb();
    private static readonly long BaseDateTicks = new DateTime(1900, 1, 1).Ticks;
    private static long _lastTimestamp;
    private static int _counter;
    
    /// <summary>
    /// Gets the instance identifier that indicates which queue process generated this message.
    /// </summary>
    /// <remarks>
    /// Each running instance of LightningQueues (assuming one per process) generates a unique identifier at startup.
    /// This helps track the origin of messages across distributed systems.
    /// </remarks>
    public Guid SourceInstanceId { get; init; }
    
    /// <summary>
    /// Gets the unique identifier specific to this message.
    /// </summary>
    /// <remarks>
    /// This identifier is generated using the COMB algorithm which embeds timestamp information
    /// into a GUID, providing both uniqueness and chronological sorting capabilities.
    /// </remarks>
    public Guid MessageIdentifier { get; init; }

    /// <summary>
    /// Generates a new random MessageId.
    /// </summary>
    /// <returns>A new MessageId with the current instance identifier and a unique message identifier.</returns>
    /// <remarks>
    /// The generated MessageId uses the COMB algorithm for the message identifier part,
    /// which embeds timestamp information for better database performance.
    /// </remarks>
    public static MessageId GenerateRandom()
    {
        return new MessageId
        {
            SourceInstanceId = InstanceId,
            MessageIdentifier = GenerateGuidComb()
        };
    }
    
    /// <summary>
    /// Generates a GUID using the COMB (Combined GUID) algorithm with timestamp-first ordering.
    /// </summary>
    /// <returns>A GUID with embedded timestamp information at the beginning.</returns>
    /// <remarks>
    /// The COMB algorithm creates GUIDs that are both unique and chronologically sortable.
    /// This method embeds the current date and time into the FIRST bytes of the GUID,
    /// which enables LMDB's MDB_APPEND optimization for sequential key inserts.
    ///
    /// Byte layout (big-endian for correct lexicographic sort):
    /// - Bytes 0-1: Days since 1900-01-01 (big-endian)
    /// - Bytes 2-5: Milliseconds of day (big-endian)
    /// - Bytes 6-7: Counter for strict ordering within same millisecond (big-endian)
    /// - Bytes 8-15: Random bytes for uniqueness
    ///
    /// This ordering ensures that newer messages have keys that sort AFTER older messages
    /// when using LMDB's default lexicographic byte comparison. The counter guarantees
    /// strict ascending order even when multiple IDs are generated within the same millisecond.
    /// </remarks>
    private static Guid GenerateGuidComb()
    {
        var guid = Guid.NewGuid();
        var now = DateTime.UtcNow;
        Span<byte> guidArray = stackalloc byte[16];
        guid.TryWriteBytes(guidArray);

        // Get the days and milliseconds which will be used to build the byte string
        var days = (ushort)(new TimeSpan(now.Ticks - BaseDateTicks).Days);
        var msecs = (uint)now.TimeOfDay.TotalMilliseconds;

        // Combine days and msecs into a single timestamp for atomic comparison
        var timestamp = ((long)days << 32) | msecs;

        // Get a counter value that ensures strict ascending order
        int counter;
        while (true)
        {
            var lastTs = Interlocked.Read(ref _lastTimestamp);
            if (timestamp > lastTs)
            {
                // New millisecond - try to reset counter
                if (Interlocked.CompareExchange(ref _lastTimestamp, timestamp, lastTs) == lastTs)
                {
                    counter = Interlocked.Exchange(ref _counter, 1);
                    counter = 0; // We got the first slot
                    break;
                }
                // Another thread beat us, retry
            }
            else
            {
                // Same or earlier millisecond - increment counter
                counter = Interlocked.Increment(ref _counter);
                if (counter <= ushort.MaxValue)
                    break;
                // Counter overflow - spin wait for next millisecond
                Thread.SpinWait(1);
                now = DateTime.UtcNow;
                msecs = (uint)now.TimeOfDay.TotalMilliseconds;
                timestamp = ((long)days << 32) | msecs;
            }
        }

        // Write days at bytes 0-1 in big-endian (most significant byte first)
        guidArray[0] = (byte)(days >> 8);
        guidArray[1] = (byte)days;

        // Write milliseconds at bytes 2-5 in big-endian
        guidArray[2] = (byte)(msecs >> 24);
        guidArray[3] = (byte)(msecs >> 16);
        guidArray[4] = (byte)(msecs >> 8);
        guidArray[5] = (byte)msecs;

        // Write counter at bytes 6-7 in big-endian for strict ordering
        guidArray[6] = (byte)(counter >> 8);
        guidArray[7] = (byte)counter;

        // Bytes 8-15 retain random values from Guid.NewGuid() for uniqueness

        return new Guid(guidArray);
    }
}