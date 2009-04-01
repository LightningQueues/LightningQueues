using System;

namespace Rhino.Queues.Protocol
{
    public interface IMessageAcceptance
    {
        void Commit();
        void Abort();
    }
}