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
        using var enumerator = items.GetEnumerator();
        while (enumerator.MoveNext())
        {
            if(cancellationToken.IsCancellationRequested)
                yield break;
            yield return await ValueTask.FromResult(enumerator.Current);
        }
    }

    internal static async IAsyncEnumerable<T> Where<T>(this IAsyncEnumerable<T> items, Func<T, bool> filter)
    {
        var enumerator = items.GetAsyncEnumerator();
        while (await enumerator.MoveNextAsync())
        {
            var current = enumerator.Current;
            if (filter(current))
                yield return current;
        }
    }
    
    internal static async IAsyncEnumerable<T> Concat<T>(this IAsyncEnumerable<T> items, IAsyncEnumerable<T> items2)
    {
        var itemsEnumerator = items.GetAsyncEnumerator();
        while (await itemsEnumerator.MoveNextAsync())
        {
            yield return itemsEnumerator.Current;
        }

        var items2Enumerator = items2.GetAsyncEnumerator();
        while (await items2Enumerator.MoveNextAsync())
        {
            yield return items2Enumerator.Current;
        }
    }
    
    internal static async IAsyncEnumerable<TResult> Select<T, TResult>(this IAsyncEnumerable<T> items, Func<T, TResult> selector)
    {
        var enumerator = items.GetAsyncEnumerator();
        while (await enumerator.MoveNextAsync())
        {
            yield return selector(enumerator.Current);
        }
    }
}