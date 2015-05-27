using System.Collections.Generic;

namespace LightningQueues.Storage
{
    public interface IStorage : IEnumerable<KeyValuePair<string, byte[]>>
    {
        byte[] Get(string key);
        void Put(string key, byte[] value);
        void Delete(string key);
        IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator(string key);
    }
}