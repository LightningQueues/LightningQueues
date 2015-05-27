using System;
using System.Collections;
using System.Collections.Generic;

namespace LightningQueues.Storage
{
    public class StorageTransaction : ITransaction
    {
        private readonly IStorage _storage;

        public StorageTransaction(IStorage storage)
        {
            _storage = storage;
            TransactionId = Guid.NewGuid();
            _storage.Put($"batch/{TransactionId}", 
                BitConverter.GetBytes(DateTime.UtcNow.ToBinary()));
        }

        public Guid TransactionId { get; }

        public void Commit()
        {
            _storage.Delete($"batch/{TransactionId}");
        }

        public void Rollback()
        {
        }

        public byte[] Get(string key)
        {
            return _storage.Get(key);
        }

        public void Put(string key, byte[] value)
        {
            _storage.Put(key, value);
        }

        public void Delete(string key)
        {
            _storage.Delete(key);
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator(string key)
        {
            return _storage.GetEnumerator(key);
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator()
        {
            return _storage.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _storage.GetEnumerator();
        }
    }
}