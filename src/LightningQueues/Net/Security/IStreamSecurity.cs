using System;
using System.IO;

namespace LightningQueues.Net.Security
{
    public interface IStreamSecurity
    {
        IObservable<Stream> Apply(Uri endpoint, IObservable<Stream> stream);
    }
}