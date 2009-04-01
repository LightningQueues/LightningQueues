namespace Rhino.Queues.Protocol
{
    public class Endpoint
    {
        public Endpoint(string host, int port)
        {
            Host = host;
            Port = port;
        }

        public string Host { get; private set; }
        public int Port { get; private set; }
    }
}