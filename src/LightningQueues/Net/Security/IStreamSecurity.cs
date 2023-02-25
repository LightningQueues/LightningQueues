using System;
using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security;

public interface IStreamSecurity
{
    IObservable<Stream> Apply(Uri endpoint, IObservable<Stream> stream);
    ValueTask<Stream> Apply(Uri endpoint, Stream stream);
}