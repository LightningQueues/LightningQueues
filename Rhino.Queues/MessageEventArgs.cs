using System;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues
{
    public class MessageEventArgs : EventArgs
    {
        public MessageEventArgs(Endpoint endpoint, Message message)
        {
            Endpoint = endpoint;
            Message = message;
        }

        public Endpoint Endpoint { get; private set; }
        public Message Message { get; private set; }
    }
}