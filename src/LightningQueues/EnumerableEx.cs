using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues;

internal static class EnumerableEx
{
    internal static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> items, 
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var item in items)
        {
            if(cancellationToken.IsCancellationRequested)
                yield break;
            var result = await ValueTask.FromResult(item);
            yield return result;
        }
    }

    internal static async IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> items, Func<T, bool> filter)
    {
        await foreach (var item in items)
        {
            if (filter(item))
                yield return item;
        }
    }
    
    internal static async IAsyncEnumerable<T> Concat<T>(this IAsyncEnumerable<T> items, IAsyncEnumerable<T> items2)
    {
        await foreach (var item in items)
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