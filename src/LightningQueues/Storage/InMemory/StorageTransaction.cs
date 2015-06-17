using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace LightningQueues.Storage.InMemory
{
    public class StorageTransaction : ITransaction, IEnumerable
    {
        private readonly IStorage _storage;
        private ConcurrentQueue<Action<IStorage>> _rollbackActions;

        public StorageTransaction(IStorage storage)
        {
            _rollbackActions = new ConcurrentQueue<Action<IStorage>>();
            _storage = storage;
            TransactionId = Guid.NewGuid();
            Put($"/batch/{TransactionId}", 
                BitConverter.GetBytes(DateTime.UtcNow.ToBinary()));
        }

        public Guid TransactionId { get; }

        public void Commit()
        {
            _storage.Delete($"/batch/{TransactionId}");
            _rollbackActions = new ConcurrentQueue<Action<IStorage>>();
        }

        public void Rollback()
        {
            foreach (var action in _rollbackActions)
            {
                action(_storage);
            }
            _rollbackActions = new ConcurrentQueue<Action<IStorage>>();
        }

        public byte[] Get(string key)
        {
            return _storage.Get(key);
        }

        public void Put(string key, byte[] value)
        {
            _rollbackActions.Enqueue(storage => storage.Delete(key));
            _storage.Put(key, value);
        }

        public void Delete(string key)
        {
            var value = _storage.Get(key);
            _rollbackActions.Enqueue(storage => storage.Put(key, value));
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