using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace LightningQueues.Net.Tcp;

public static class NetExtensions
{
    public static IAsyncEnumerable<IList<TSource>> Buffer<TSource>(this IAsyncEnumerable<TSource> source,
        TimeSpan timeSpan, int count, CancellationToken cancellation = default)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (timeSpan < TimeSpan.Zero) throw new ArgumentNullException(nameof(timeSpan));
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count));
        return Implementation(cancellation);

        async IAsyncEnumerable<IList<TSource>> Implementation(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var timerCts = new CancellationTokenSource();
            var delayTask = Task.Delay(timeSpan, timerCts.Token);
            (ValueTask<bool> ValueTask, Task<bool> Task) moveNext = default;
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var enumerator = source.GetAsyncEnumerator(linkedCts.Token);
            try
            {
                moveNext = (enumerator.MoveNextAsync(), null);
                var buffer = new List<TSource>(count);
                ExceptionDispatchInfo error = null;
                while (true)
                {
                    Task completedTask = null;
                    if (!moveNext.ValueTask.IsCompleted)
                    {
                        // Preserve the ValueTask, if it's not preserved already.
                        if (moveNext.Task == null)
                        {
                            var preserved = moveNext.ValueTask.AsTask();
                            moveNext = (new ValueTask<bool>(preserved), preserved);
                        }

                        completedTask = await Task.WhenAny(moveNext.Task, delayTask)
                            .ConfigureAwait(false);
                    }

                    if (completedTask == delayTask)
                    {
                        Debug.Assert(delayTask.IsCompleted);
                        yield return buffer.ToArray(); // It's OK if the buffer is empty.
                        buffer.Clear();
                        delayTask = Task.Delay(timeSpan, timerCts.Token);
                    }
                    else
                    {
                        Debug.Assert(moveNext.ValueTask.IsCompleted);
                        // Await a copy, to prevent a second await on finally.
                        var moveNextCopy = moveNext.ValueTask;
                        moveNext = default;
                        bool moved;
                        try
                        {
                            moved = await moveNextCopy.ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            error = ExceptionDispatchInfo.Capture(ex);
                            break;
                        }

                        if (!moved) break;
                        buffer.Add(enumerator.Current);
                        if (buffer.Count == count)
                        {
                            timerCts.Cancel();
                            timerCts.Dispose();
                            timerCts = new CancellationTokenSource();
                            yield return buffer.ToArray();
                            buffer.Clear();
                            delayTask = Task.Delay(timeSpan, timerCts.Token);
                        }

                        try
                        {
                            moveNext = (enumerator.MoveNextAsync(), null);
                        }
                        catch (Exception ex)
                        {
                            error = ExceptionDispatchInfo.Capture(ex);
                            break;
                        }
                    }
                }

                if (buffer.Count > 0) yield return buffer.ToArray();
                error?.Throw();
            }
            finally
            {
                // The finally runs when an enumerator created by this method is disposed.
                timerCts.Cancel();
                timerCts.Dispose();
                // Prevent fire-and-forget, otherwise the DisposeAsync() might throw.
                // Cancel the async-enumerator, for more responsive completion.
                // Swallow MoveNextAsync errors, but propagate DisposeAsync errors.
                linkedCts.Cancel();
                try
                {
                    await moveNext.ValueTask.ConfigureAwait(false);
                }
                catch
                {
                }

                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}