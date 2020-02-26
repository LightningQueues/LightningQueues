using System;
using LightningQueues.Storage.LMDB;
using System.Collections.Generic;
using System.Net;
using System.Net.Security;
using System.Reactive.Concurrency;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using LightningQueues.Logging;
using LightningQueues.Storage;
using Xunit;

namespace LightningQueues.Tests
{
    public static class ObjectMother
    {
        public  static T NewMessage<T>(string queueName = "cleverqueuename", string payload = "hello", string headerValue = "myvalue") where T : Message, new()
        {
            var message = new T
            {
                Data = Encoding.UTF8.GetBytes(payload),
                Headers = new Dictionary<string, string>
                {
                    {"mykey", headerValue}
                },
                Id = MessageId.GenerateRandom(),
                Queue = queueName,
                SentAt = DateTime.Now
            };
            return message;

        }

        public static Queue NewQueue(string path = null, string queueName = "test", ILogger logger = null, 
            IScheduler scheduler = null, IMessageStore store = null, bool secureTransport = false)
        {
            store ??= new LmdbMessageStore(path);
            var queueConfiguration = new QueueConfiguration();
            queueConfiguration.LogWith(logger ?? new RecordingLogger());
            queueConfiguration.AutomaticEndpoint();
            queueConfiguration.ScheduleQueueWith(scheduler ?? TaskPoolScheduler.Default);
            queueConfiguration.StoreMessagesWith(store);
            if (secureTransport)
            {
                var certificate = CreateCertificate();
                queueConfiguration.SecureTransportWith(async (_, receiving) =>
                {
                    var sslStream = new SslStream(receiving, false);
                    try
                    {
                        await sslStream.AuthenticateAsServerAsync(certificate, false,
                            checkCertificateRevocation: false, enabledSslProtocols: SslProtocols.Tls12);
                    }
                    catch (Exception ex)
                    {
                        Assert.True(false, "Error occurred from receiving encryption {ex}");
                    }
                    return sslStream;
                }, async (uri, sending) =>
                {
                    bool ValidateServerCertificate(object sender, X509Certificate cert, X509Chain chain,
                        SslPolicyErrors sslPolicyErrors)
                    {
                        return true; //we only care that the transport is encrypted
                    }

                    var sslStream = new SslStream(sending, true, ValidateServerCertificate, null);
                    
                    try
                    {
                        await sslStream.AuthenticateAsClientAsync(uri.Host);
                    }
                    catch (Exception ex)
                    {
                        Assert.True(false, $"Authenticating as a client failed for {ex}");
                    }

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
            SubjectAlternativeNameBuilder sanBuilder = new SubjectAlternativeNameBuilder();
            sanBuilder.AddIpAddress(IPAddress.Loopback);
            sanBuilder.AddIpAddress(IPAddress.IPv6Loopback);
            sanBuilder.AddDnsName("localhost");
            sanBuilder.AddDnsName(Environment.MachineName);
            var certificateName = "LightningQueues";
            var certPass = "reallysecure";

            X500DistinguishedName distinguishedName = new X500DistinguishedName($"CN={certificateName}");

            using (RSA rsa = RSA.Create(4096))
            {
                var request = new CertificateRequest(distinguishedName, rsa, HashAlgorithmName.SHA256,
                    RSASignaturePadding.Pkcs1);

                request.CertificateExtensions.Add(
                    new X509KeyUsageExtension(
                        X509KeyUsageFlags.DataEncipherment | X509KeyUsageFlags.KeyEncipherment |
                        X509KeyUsageFlags.DigitalSignature, false));

                request.CertificateExtensions.Add(
                    new X509EnhancedKeyUsageExtension(
                        new OidCollection {new Oid("1.3.6.1.5.5.7.3.1")}, false));

                request.CertificateExtensions.Add(sanBuilder.Build());

                var certificate = request.CreateSelfSigned(new DateTimeOffset(DateTime.UtcNow.AddDays(-1)),
                    new DateTimeOffset(DateTime.UtcNow.AddDays(3650)));

                return new X509Certificate2(certificate.Export(X509ContentType.Pfx, certPass), certPass,
                    X509KeyStorageFlags.MachineKeySet);
            }
        }
    }
}