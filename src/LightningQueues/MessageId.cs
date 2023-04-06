using System;
using System.Runtime.InteropServices;

namespace LightningQueues;

public record MessageId
{
    private static readonly Guid InstanceId = GenerateGuidComb();
    public Guid SourceInstanceId { get; init; }
    public Guid MessageIdentifier { get; init; }

    public static MessageId GenerateRandom()
    {
        return new MessageId
        {
            SourceInstanceId = InstanceId,
            MessageIdentifier = GenerateGuidComb()
        };
    }
    private static readonly long BaseDateTicks = new DateTime(1900, 1, 1).Ticks;
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
        MemoryMarshal.Write(daysArray, ref days);

        // Note that SQL Server is accurate to 1/300th of a millisecond so we divide by 3.333333
        var msecsSql = (long)(msecs / 3.333333);

        Span<byte> msecsArray = stackalloc byte[8];
        MemoryMarshal.Write(msecsArray, ref msecsSql);

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