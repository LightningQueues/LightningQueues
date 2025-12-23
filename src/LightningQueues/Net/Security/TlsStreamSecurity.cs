using System;
using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security;

public class TlsStreamSecurity(Func<Uri, Stream, Task<Stream>> streamSecurity) : IStreamSecurity
{
    public async ValueTask<Stream> Apply(Uri endpoint, Stream stream)
    {
        return await streamSecurity(endpoint, stream).ConfigureAwait(false);
    }
}