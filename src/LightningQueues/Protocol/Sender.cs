using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using LightningQueues.Exceptions;
using LightningQueues.Logging;
using LightningQueues.Model;

namespace LightningQueues.Protocol
{
    public class Sender
    {
        private static readonly ILogger _logger = LogManager.GetLogger<Sender>();

        public Action Success { get; set; }
        public Action Connected { get; set; }
        public Endpoint Destination { get; set; }
        public Message[] Messages { get; set; }

        public Sender()
        {
            Connected = () => { };
            Success = () => { };
        }

        public async Task Send()
        {
            _logger.Debug("Starting to send {0} messages to {1}", Messages.Length, Destination);
            using (var client = new TcpClient())
            {
                await Connect(client).ConfigureAwait(false);

                using (var stream = client.GetStream())
                {
                    await new SendingProtocol()
                        .Send(stream, Success, Messages, Destination.ToString())
                        .WithTimeout(TimeSpan.FromSeconds(5))
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task Connect(TcpClient client)
        {
            try
            {
                await client.ConnectAsync(Destination.Host, Destination.Port).ConfigureAwait(false);
                Connected();
                _logger.Debug("Successfully connected to {0}", Destination);
            }
            catch (Exception ex)
            {
                throw new FailedToConnectException("Failed to connect", ex);
            }
        }
    }
}
