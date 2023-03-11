using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LightningQueues.Net.Protocol;

public static class StreamExtensions
{
    public static async Task<byte[]> ReadBytesAsync(this Stream stream, int length)
    {
        var buffer = new byte[length];
        var totalRead = 0;
        int current;
        do
        {
            current = await stream.ReadAsync(buffer, totalRead, buffer.Length - totalRead).ConfigureAwait(false);
            totalRead += current;
        }
        while (totalRead < length && current > 0);
        return buffer;
    }
}