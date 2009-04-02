using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using log4net;
using Wintellect.Threading.AsyncProgModel;

namespace Rhino.Queues.Protocol
{
    public class StreamUtil
    {
        public static IEnumerator<int> ReadBytes(
            byte[] buffer, Stream stream, AsyncEnumerator ae, string type, bool expectedToHaveNoData)
        {
            var totalBytesRead = 0;

            while (totalBytesRead < buffer.Length)
            {
                stream.BeginRead(buffer, totalBytesRead, buffer.Length - totalBytesRead, ae.End(), null);
               
                yield return 1;

                int bytesRead = stream.EndRead(ae.DequeueAsyncResult());

                if(bytesRead == 0)
                {
                    if (expectedToHaveNoData)
                        yield break;

                    throw new InvalidOperationException("Could not read value for " + type);
                }

                totalBytesRead += bytesRead;
            }
        }
        
    }
}