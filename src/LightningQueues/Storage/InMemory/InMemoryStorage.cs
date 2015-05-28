using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace LightningQueues.Storage.InMemory
{
    public class InMemoryStorage : IStorage
    {
        readonly ConcurrentDictionary<string, byte[]> _items;

        public InMemoryStorage()
        {
            _items = new ConcurrentDictionary<string, byte[]>();
        }

        public byte[] Get(string key)
        {
            if (!_items.ContainsKey(key))
                return null;
            return _items[key];
        }

        public void Put(string key, byte[] value)
        {
            _items.AddOrUpdate(key, value, (k, v) => value);
        }

        public void Delete(string key)
        {
            byte[] nothing;
            _items.TryRemove(key, out nothing);
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator()
        {
            return new SortedList<string, byte[]>(_items).GetEnumerator();
        }

        public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator(string key)
        {
            return new SortedList<string, byte[]>(_items).GetEnumerator(key);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _items.GetEnumerator();
        }
    }
}