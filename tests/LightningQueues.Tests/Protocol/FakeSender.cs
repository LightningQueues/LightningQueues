using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using LightningQueues.Exceptions;
using LightningQueues.Logging;
using LightningQueues.Model;
using LightningQueues.Protocol;
using LightningQueues.Protocol.Chunks;
using LightningQueues.Storage;
using LogManager = LightningQueues.Logging.LogManager;

namespace LightningQueues.Tests.Protocol
{
    public class FakeSender
    {
        private readonly ILogger _logger = LogManager.GetLogger<FakeSender>();
        public event Action SendCompleted;
        public Func<MessageBookmark[]> Success { get; set; }
        public Action<Exception> Failure { get; set; }
        public Action<MessageBookmark[]> Revert { get; set; }
        public Action Connected { get; set; }
        public Action<Exception> FailureToConnect { get; set; }
        public Action Commit { get; set; }
        public Endpoint Destination { get; set; }
        public Message[] Messages { get; set; }

        public FakeSender()
        {
            Connected = () => { };
            FailureToConnect = e => { };
            Failure = e => { };
            Success = () => null;
            Revert = bookmarks => { };
            Commit = () => { };
        }

        public Task Send()
        {
            _logger.Debug("Starting to send {0} messages to {1}", Messages.Length, Destination);
            return SendInternalAsync();
        }

        private async Task SendInternalAsync()
        {
            using (var client = new TcpClient())
            {
                try
                {
                    var connected = await Connect(client);
                    if (!connected)
                        return;

                    using (var stream = client.GetStream())
                    {
                        var buffer = Messages.Serialize();
                        MessageBookmark[] bookmarks = null;
                        try
                        {
                            await new WriteLength(_logger, buffer.Length, Destination.ToString()).ProcessAsync(stream);
                            await new WriteMessage(_logger, buffer, Destination.ToString()).ProcessAsync(stream);
                            await new ReadReceived(_logger, Destination.ToString()).ProcessAsync(stream);
                            //bookmarks = Success();
                            //await new ReadRevert(Destination.ToString()).ProcessAsync(stream);
                            //Commit();
                        }
                        catch (RevertSendException)
                        {
                            _logger.Info("Got back revert message from receiver {0}, reverting send", Destination);
                            Revert(bookmarks);
                        }
                        catch (Exception exception)
                        {
                            Failure(exception);
                        }
                    }
                }
                finally
                {
                    var completed = SendCompleted;
                    if (completed != null)
                        completed();
                }
            }
        }

        private async Task<bool> Connect(TcpClient client)
        {
            try
            {
                await client.ConnectAsync(Destination.Host, Destination.Port);
            }
            catch (Exception exception)
            {
                _logger.Info("Failed to connect to {0} because {1}", Destination, exception);
                FailureToConnect(exception);
                return false;
            }

            _logger.Debug("Successfully connected to {0}", Destination);
            return true;
        }
    }
}
