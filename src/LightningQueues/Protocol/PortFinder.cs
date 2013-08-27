using System.Linq;
using System.Net.NetworkInformation;

namespace LightningQueues.Protocol
{
    public class PortFinder
    {
         public int Find()
         {
             const int START_OF_IANA_PRIVATE_PORT_RANGE = 49152;
             var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
             var tcpListeners = ipGlobalProperties.GetActiveTcpListeners();
             var tcpConnections = ipGlobalProperties.GetActiveTcpConnections();

             var allInUseTcpPorts = tcpListeners.Select(tcpl => tcpl.Port)
                 .Union(tcpConnections.Select(tcpi => tcpi.LocalEndPoint.Port));

             var orderedListOfPrivateInUseTcpPorts = allInUseTcpPorts
                 .Where(p => p >= START_OF_IANA_PRIVATE_PORT_RANGE)
                 .OrderBy(p => p);

             var candidatePort = START_OF_IANA_PRIVATE_PORT_RANGE;
             foreach (var usedPort in orderedListOfPrivateInUseTcpPorts)
             {
                 if (usedPort != candidatePort) break;
                 candidatePort++;
             }
             return candidatePort;
         }
    }
}