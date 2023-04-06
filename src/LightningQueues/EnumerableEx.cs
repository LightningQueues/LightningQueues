using System;
using System.Collections.Generic;

namespace LightningQueues;

internal static class EnumerableEx
{
    internal static async IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> items, Func<T, bool> filter)
    {
        await foreach (var item in items)
        {
            if (filter(item))
                yield return item;
        }
    }
    
    internal static async IAsyncEnumerable<T> Concat<T>(this IEnumerable<T> items, IAsyncEnumerable<T> items2)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await foreach (var item in items2)
        {
            yield return item;
        }
    }
    
    internal static async IAsyncEnumerable<TResult> Select<T, TResult>(this IAsyncEnumerable<T> items, Func<T, TResult> selector)
    {
        await foreach (var item in items)
        {
            yield return selector(item);
        }
    }
}