using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using LightningQueues.Net.Security;

namespace LightningQueues.Builders;

public class ServerTLSSecurity : IStreamSecurity
{
    private readonly X509Certificate _certificate;

    public ServerTLSSecurity(X509Certificate certificate)
    {
        _certificate = certificate;
    }
    
    public async ValueTask<Stream> Apply(Uri endpoint, Stream stream)
    {
        var sslStream = new SslStream(stream, false);
        await sslStream.AuthenticateAsServerAsync(_certificate, false,
            checkCertificateRevocation: false, enabledSslProtocols: SslProtocols.Tls12);
        return sslStream;
    }
}