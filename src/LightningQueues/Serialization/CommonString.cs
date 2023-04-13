using System.Buffers;

namespace LightningQueues.Serialization;

internal class CommonString
{
    public CommonString(string value, ReadOnlySequence<byte> bytes)
    {
        Value = value;
        Bytes = bytes;
    }
    public readonly string Value; 
    public readonly ReadOnlySequence<byte> Bytes;
}