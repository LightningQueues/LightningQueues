using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LightningQueues.Net.Protocol
{
    public static class StreamExtensions
    {
        public static async Task<byte[]> ReadBytesAsync(this Stream stream, int length)
        {
            byte[] buffer = new byte[length];
            int totalRead = 0;
            int current;
            do
            {
                current = await stream.ReadAsync(buffer, 0, buffer.Length - totalRead).ConfigureAwait(false);
                totalRead += current;
            }
            while (totalRead < buffer.Length && current > 0);
            return buffer;
        }

        public static async Task<bool> ReadExpectedBuffer(this Stream stream, byte[] expected)
        {
            var bytes = await stream.ReadBytesAsync(expected.Length);
            return expected.SequenceEqual(bytes);
        }
    }
}