using System;
using System.IO;
using System.Threading.Tasks;

namespace LightningQueues.Net.Security;

public interface IStreamSecurity
{
    ValueTask<Stream> Apply(Uri endpoint, Stream stream);
}