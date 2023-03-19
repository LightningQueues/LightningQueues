using System;

namespace LightningQueues.Storage;

public interface ITransaction : IDisposable
{
    void Commit();
}