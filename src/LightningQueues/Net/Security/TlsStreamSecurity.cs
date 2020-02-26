using System;
using System.IO;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security
{
    public class TlsStreamSecurity : IStreamSecurity
    {
        private readonly Func<Uri, Stream, Task<Stream>> _streamSecurity;

        public TlsStreamSecurity(Func<Uri, Stream, Task<Stream>> streamSecurity)
        {
            _streamSecurity = streamSecurity;
        }
        
        public IObservable<Stream> Apply(Uri endpoint, IObservable<Stream> stream)
        {
            return stream.SelectMany(x => _streamSecurity(endpoint, x));
        }
    }
}