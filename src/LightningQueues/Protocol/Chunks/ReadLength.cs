using System;
using System.IO;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Exceptions;

namespace LightningQueues.Protocol.Chunks
{
    public class ReadLength : Chunk<byte[]>
    {
        public ReadLength(ILogger logger, string sender) : base(logger, sender)
        {
        }

        public ReadLength(ILogger logger) : this(logger, null)
        {
        }

        protected override async Task<byte[]> GetInternalAsync(Stream stream)
        {
            var lenOfDataToReadBuffer = new byte[sizeof(int)];
            await stream.ReadBytesAsync(lenOfDataToReadBuffer, "length data", false);

            var lengthOfDataToRead = BitConverter.ToInt32(lenOfDataToReadBuffer, 0);
            if (lengthOfDataToRead < 0)
            {
                throw new InvalidLengthException(string.Format("Got invalid length {0} from endpoint {1}",
                                                               lengthOfDataToRead, _endpoint));
            }

            var buffer = new byte[lengthOfDataToRead];
            return buffer;
        }

        public override string ToString()
        {
            return "Reading Length ";
        }
    }
}