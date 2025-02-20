using System;
using System.Collections.Generic;

namespace LightningQueues.Serialization;

public class MemStringEqualityComparer : IEqualityComparer<ReadOnlyMemory<char>>
{
    public int GetHashCode( ReadOnlyMemory<char> obj ) =>
        string.GetHashCode( obj.Span, StringComparison.CurrentCulture );
    public bool Equals(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) =>
        x.Span.Equals(y.Span, StringComparison.CurrentCulture );
}