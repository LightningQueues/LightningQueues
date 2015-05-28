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

        public static IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator(this SortedList<string, byte[]> list, string keyStart)
        {
            var keys = list.Keys.ToList();
            var values = list.Values.ToList();
            for (var i = list.IndexOfKey(keyStart); i < list.Count && i >= 0; i++)
            {
                var key = keys[i];
                if(!key.StartsWith(keyStart))
                    yield break;
                yield return new KeyValuePair<string, byte[]>(keys[i], values[i]);
            }
        }
    }
}