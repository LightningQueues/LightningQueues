using LightningQueues.Storage.LMDB;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using LightningDB;
using LightningQueues.Serialization;
using Microsoft.Extensions.Logging;
using LightningQueues.Storage;

namespace LightningQueues.Builders;

public static class QueueBuilder
{
    public  static T NewMessage<T>(string queueName = "clever_queue_name", string payload = "hello") where T : Message, new()
    {
        var message = new T
        {
            Data = Encoding.UTF8.GetBytes(payload),
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
        var serializer = new MessageSerializer();
        if (store == null)
        {
            var environment = new LightningEnvironment(path, new EnvironmentConfiguration
            {
                MapSize = 1024 * 1024 * 100,
                MaxDatabases = 5
            });
            environment.Open(EnvironmentOpenFlags.NoLock);
            store ??= new LmdbMessageStore(environment, serializer);
        }

        var queueConfiguration = new QueueConfiguration();
        queueConfiguration.LogWith(logger)
            .AutomaticEndpoint()
            .SerializeWith(serializer)
            .StoreMessagesWith(store);
        if (timeoutAfter.HasValue)
        {
            queueConfiguration.TimeoutNetworkBatchAfter(timeoutAfter.Value);
        }
        if (secureTransport)
        {
            var certificate = CreateCertificate();
            queueConfiguration.SecureTransportWith(new ClientTLSSecurity(certificate), 
                new ServerTLSSecurity(certificate));
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