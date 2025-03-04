using System;
using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security;

public class TlsStreamSecurity : IStreamSecurity
{
    private readonly Func<Uri, Stream, Task<Stream>> _streamSecurity;

    public TlsStreamSecurity(Func<Uri, Stream, Task<Stream>> streamSecurity)
    {
        _streamSecurity = streamSecurity;
    }
        
    public async ValueTask<Stream> Apply(Uri endpoint, Stream stream)
    {
        return await _streamSecurity(endpoint, stream).ConfigureAwait(false);
    }
}