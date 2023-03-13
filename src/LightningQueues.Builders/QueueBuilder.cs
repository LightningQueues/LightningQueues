using LightningQueues.Storage.LMDB;
using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Builders;

public static class QueueBuilder
{
    public  static T NewMessage<T>(string queueName = "clever_queue_name", string payload = "hello", string headerValue = "my_value") where T : Message, new()
    {
        var message = new T
        {
            Data = Encoding.UTF8.GetBytes(payload),
            Headers = new Dictionary<string, string>
            {
                {"my_key", headerValue}
            },
            Id = MessageId.GenerateRandom(),
            Queue = queueName,
            SentAt = DateTime.Now
        };
        return message;
    }

    public static Queue NewQueue(string path = null, string queueName = "test", ILogger logger = null, 
        IMessageStore store = null, bool secureTransport = false, TimeSpan? timeoutAfter = null)
    {
        logger ??= new RecordingLogger();
        store ??= new LmdbMessageStore(path);
        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.LogWith(logger);
        queueConfiguration.AutomaticEndpoint();
        queueConfiguration.StoreMessagesWith(store);
        if (timeoutAfter.HasValue)
        {
            queueConfiguration.TimeoutNetworkBatchAfter(timeoutAfter.Value);
        }
        if (secureTransport)
        {
            var certificate = CreateCertificate();
            queueConfiguration.SecureTransportWith(async (_, receiving) =>
            {
                var sslStream = new SslStream(receiving, false);
                await sslStream.AuthenticateAsServerAsync(certificate, false,
                    checkCertificateRevocation: false, enabledSslProtocols: SslProtocols.Tls12);
                return sslStream;
            }, async (uri, sending) =>
            {
                bool ValidateServerCertificate(object sender, X509Certificate cert, X509Chain chain,
                    SslPolicyErrors sslPolicyErrors)
                {
                    return true; //we only care that the transport is encrypted
                }

                var sslStream = new SslStream(sending, true, ValidateServerCertificate, null);
                await sslStream.AuthenticateAsClientAsync(uri.Host);
                return sslStream;
            });
        }
        var queue = queueConfiguration.BuildQueue();
        queue.CreateQueue(queueName);
        queue.Start();
        return queue;
    }

    //https://stackoverflow.com/questions/42786986/how-to-create-a-valid-self-signed-x509certificate2-programmatically-not-loadin
    private static X509Certificate2 CreateCertificate()
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

        return new X509Certificate2(certificate.Export(X509ContentType.Pfx, certPass), certPass,
            X509KeyStorageFlags.MachineKeySet);
    }
}