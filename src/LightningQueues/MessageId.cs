using System;
using System.Runtime.InteropServices;

namespace LightningQueues;

/// <summary>
/// Represents a unique identifier for messages in the LightningQueues system.
/// </summary>
/// <remarks>
/// MessageId combines a source instance identifier and a message-specific identifier
/// to ensure global uniqueness. The identifiers use a COMB (Combined GUID) approach
/// which embeds timestamp information for better database clustering.
/// </remarks>
public record MessageId
{
    private static readonly Guid InstanceId = GenerateGuidComb();
    private static readonly long BaseDateTicks = new DateTime(1900, 1, 1).Ticks;
    
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
    /// Generates a GUID using the COMB (Combined GUID) algorithm.
    /// </summary>
    /// <returns>A GUID with embedded timestamp information.</returns>
    /// <remarks>
    /// The COMB algorithm creates GUIDs that are both unique and chronologically sortable.
    /// This method embeds the current date and time into parts of a randomly generated GUID,
    /// which helps with database index efficiency while maintaining uniqueness.
    /// 
    /// The algorithm is based on the approach used by SQL Server for NEWSEQUENTIALID().
    /// </remarks>
    private static Guid GenerateGuidComb()
    {
        var guid = Guid.NewGuid();
        var now = DateTime.Now;
        Span<byte> guidArray = stackalloc byte[16];
        guid.TryWriteBytes(guidArray);

        // Get the days and milliseconds which will be used to build the byte string
        var days = new TimeSpan(now.Ticks - BaseDateTicks).Days;
        var msecs = now.TimeOfDay.TotalMilliseconds;

        // Convert to a byte array
        Span<byte> daysArray = stackalloc byte[4];
        MemoryMarshal.Write(daysArray, in days);

        // Note that SQL Server is accurate to 1/300th of a millisecond so we divide by 3.333333
        var msecsSql = (long)(msecs / 3.333333);

        Span<byte> msecsArray = stackalloc byte[8];
        MemoryMarshal.Write(msecsArray, in msecsSql);

        // Reverse the bytes to match SQL Servers ordering
        // Copy the bytes into the guid
        guidArray[15] = msecsArray[0];
        guidArray[14] = msecsArray[1];
        guidArray[13] = msecsArray[2];
        guidArray[12] = msecsArray[3];
        guidArray[11] = daysArray[0];
        guidArray[10] = daysArray[1];

        return new Guid(guidArray);
    }
}