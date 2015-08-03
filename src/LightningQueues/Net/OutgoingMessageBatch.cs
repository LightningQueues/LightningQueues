using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace LightningQueues.Net
{
    public class OutgoingMessageBatch : IDisposable
    {
        public OutgoingMessageBatch(Uri destination, IEnumerable<OutgoingMessage> messages, TcpClient client)
        {
            Destination = destination;
            var messagesList = new List<OutgoingMessage>();
            messagesList.AddRange(messages);
            Messages = messagesList;
            Client = client;
        }

        public Uri Destination { get; set; }
        public Stream Stream => Client.GetStream();
        public TcpClient Client { get; set; }
        public IList<OutgoingMessage> Messages { get; }

        public Task ConnectAsync()
        {
            return Client.ConnectAsync(Destination.Host, Destination.Port);
        }

        public void Dispose()
        {
            using (Client)
            {
            }
            Client = null;
        }
    }
}