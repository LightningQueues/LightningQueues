using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using LightningDB;
using LightningQueues.Logging;
using LightningQueues.Net.Security;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Tests;

public static class QueueConfigurationExtensions
{
    public static QueueConfiguration WithDefaultsForTest(this QueueConfiguration configuration, TextWriter console = null)
    {
        configuration.WithDefaults();
        configuration.StoreWithLmdb(TestBase.TempPath(), new EnvironmentConfiguration
        {
            MapSize = 1024 * 1024 * 100,
            MaxDatabases = 5
        });
        configuration.LogWith(new RecordingLogger(console));
        return configuration;
    }

    public static QueueConfiguration WithSelfSignedCertificateSecurity(this QueueConfiguration configuration)
    {
        //https://stackoverflow.com/questions/42786986/how-to-create-a-valid-self-signed-x509certificate2-programmatically-not-loadin
        static X509Certificate2 CreateCertificate()
        {
            var sanBuilder = new SubjectAlternativeNameBuilder();
            sanBuilder.AddIpAddress(IPAddress.Loopback);
            sanBuilder.AddIpAddress(IPAddress.IPv6Loopback);
            sanBuilder.AddDnsName("localhost");
            sanBuilder.AddDnsName(Environment.MachineName);
            const string certificateName = "LightningQueues";
            const string certPass = "really_secure";

            var distinguishedName = new X500DistinguishedName($"CN={certificateName}");
    
            using var rsa = RSA.Create(4096);
            var request = new CertificateRequest(distinguishedName, rsa, HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
    
            request.CertificateExtensions.Add(
                new X509KeyUsageExtension(
                    X509KeyUsageFlags.DataEncipherment | X509KeyUsageFlags.KeyEncipherment |
                    X509KeyUsageFlags.DigitalSignature, false));
    
            request.CertificateExtensions.Add(
                new X509EnhancedKeyUsageExtension(
                    new OidCollection {new("1.3.6.1.5.5.7.3.1")}, false));
    
            request.CertificateExtensions.Add(sanBuilder.Build());
    
            var certificate = request.CreateSelfSigned(new DateTimeOffset(DateTime.UtcNow.AddDays(-1)),
                new DateTimeOffset(DateTime.UtcNow.AddDays(3650)));
    
            return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx, certPass), certPass);
        }
        
        var certificate = CreateCertificate();
        configuration.SecureTransportWith(new TlsStreamSecurity(async (uri, stream) =>
            {
                //client side with no validation of server certificate
                var sslStream = new SslStream(stream, true, (_, _, _, _) => true, null);
                await sslStream.AuthenticateAsClientAsync(uri.Host);
                return sslStream;
            }),
            new TlsStreamSecurity(async (_, stream) =>
            {
                var sslStream = new SslStream(stream, false);
                await sslStream.AuthenticateAsServerAsync(certificate, false,
                    checkCertificateRevocation: false, enabledSslProtocols: SslProtocols.Tls12);
                return sslStream;
            }));
        return configuration;
    }
    
    public static Queue BuildAndStartQueue(this QueueConfiguration configuration, params string[] queues)
    {
        var queue = configuration.BuildQueue();
        foreach (var queueName in queues)
        {
            queue.CreateQueue(queueName);
        }
        queue.Start();
        return queue;
    }
}