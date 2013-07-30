using System;

namespace Rhino.Queues.Internal
{
    public interface ITransaction
    {
        Guid Id { get; }
        void Rollback();
        void Commit();
    }
}