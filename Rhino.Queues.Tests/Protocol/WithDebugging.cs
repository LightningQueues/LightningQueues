using Common.Logging;
using Common.Logging.Simple;

namespace Rhino.Queues.Tests.Protocol
{
    public class WithDebugging
    {
        static WithDebugging()
        {
            LogManager.Adapter = new ConsoleOutLoggerFactoryAdapter();
        }
    }
}