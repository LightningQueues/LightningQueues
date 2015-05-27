using System;
using System.Collections;
using System.Collections.Generic;

namespace LightningQueues.Storage
{
    public class NulloTransaction : ITransaction
    {
        public Guid TransactionId { get; }

        public void Commit()
        {
        }

        public void Rollback()
        {
        }

        public byte[] Get(string key)
        {
            return null;
        }

        public void Put(string key, byte[] value)
        {
        }

        public void Delete(string key)
        {
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator(string key)
        {
            yield break;
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator()
        {
            yield break;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
