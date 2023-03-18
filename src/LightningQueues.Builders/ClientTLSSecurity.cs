using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using LightningQueues.Net.Security;

namespace LightningQueues.Builders;

public class ClientTLSSecurity : IStreamSecurity
{
    private readonly X509Certificate _certificate;

    public ClientTLSSecurity(X509Certificate certificate)
    {
        _certificate = certificate;
    }
    
    public async ValueTask<Stream> Apply(Uri endpoint, Stream stream)
    {
        var sslStream = new SslStream(stream, true, ValidateServerCertificate, null);
        await sslStream.AuthenticateAsClientAsync(endpoint.Host);
        return sslStream;
    }
    
    private static bool ValidateServerCertificate(object sender, X509Certificate cert, X509Chain chain,
        SslPolicyErrors sslPolicyErrors)
    {
        return true; //we only care that the transport is encrypted
    }
}