using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Exceptions;
using LightningQueues.Model;

namespace LightningQueues.Protocol
{
    public class Sender
    {
        private readonly ILogger _logger;

        public Action Success { get; set; }
        public Action Connected { get; set; }
        public Endpoint Destination { get; set; }
        public Message[] Messages { get; set; }

        public Sender(ILogger logger)
        {
            _logger = logger;
            Connected = () => { };
            Success = () => { };
        }

        public async Task Send()
        {
            _logger.Debug("Starting to send {0} messages to {1}", Messages.Length, Destination);
            using (var client = new TcpClient())
            {
                await Connect(client);

                using (var stream = client.GetStream())
                {
                    await new SendingProtocol(_logger)
                        .Send(stream, Success, Messages, Destination.ToString());
                }
            }
        }

        private async Task Connect(TcpClient client)
        {
            try
            {
                await client.ConnectAsync(Destination.Host, Destination.Port);
                _logger.Debug("Successfully connected to {0}", Destination);
            }
            catch (Exception ex)
            {
                throw new FailedToConnectException("Failed to connect", ex);
            }
        }
    }
}
