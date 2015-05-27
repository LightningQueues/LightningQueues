using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNet.WebUtilities;

namespace LightningQueues.Storage
{
    public static class UtilityExtensions
    {
        public static string ToQueryString(this IDictionary<string, string> queryString)
        {
            return QueryHelpers.AddQueryString(string.Empty, queryString);
        }

        public static IDictionary<string, string> ParseQueryString(this string queryString)
        {
            return QueryHelpers.ParseQuery(queryString)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.First());
        }

        public static IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator<TKey, TValue>(this SortedList<TKey, TValue> list, TKey key)
        {
            var keys = list.Keys.ToList();
            var values = list.Values.ToList();
            for (var i = list.IndexOfKey(key); i < list.Count && i >= 0; i++)
            {
                yield return new KeyValuePair<TKey, TValue>(keys[i], values[i]);
            }
        }
    }
}