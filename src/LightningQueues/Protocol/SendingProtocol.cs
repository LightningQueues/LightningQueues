using System;
using System.IO;
using System.Threading.Tasks;
using FubuCore.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol.Chunks;

namespace LightningQueues.Protocol
{
    public class SendingProtocol
    {
        private readonly ILogger _logger;

        public SendingProtocol(ILogger logger)
        {
            _logger = logger;
        }

        public async Task Send(Stream stream, Action success, Message[] messages, string destination)
        {
            var buffer = messages.Serialize();
            await new WriteLength(_logger, buffer.Length, destination).ProcessAsync(stream);
            await new WriteMessage(_logger, buffer, destination).ProcessAsync(stream);
            await new ReadReceived(_logger, destination).ProcessAsync(stream);
            await new WriteAcknowledgement(_logger, destination).ProcessAsync(stream);
            success();
            await new ReadRevert(_logger, destination).ProcessAsync(stream);
        }
    }
}