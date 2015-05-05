using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Protocol
{
    public static class StreamExtensions
    {
        public static async Task ReadAsyncToEnd(this Stream stream, byte[] buffer)
        {
            int totalRead = 0;
            do
            {
                totalRead += await stream.ReadAsync(buffer, 0, buffer.Length - totalRead).ConfigureAwait(false);
            }
            while (totalRead < buffer.Length && stream.CanRead);
        }

        public static async Task<byte[]> ReadBytesAsync(this Stream stream, int length)
        {
            byte[] buffer = new byte[length];
            await stream.ReadAsyncToEnd(buffer);
            return buffer;
        }
    }
}