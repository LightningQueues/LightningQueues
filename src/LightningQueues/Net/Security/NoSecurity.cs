using System;
using System.IO;

namespace LightningQueues.Net.Security
{
    public class NoSecurity : IStreamSecurity
    {
        public IObservable<Stream> Apply(Uri endpoint, IObservable<Stream> stream)
        {
            return stream;
        }
    }
}