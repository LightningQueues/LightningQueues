using System;
using System.Text;

namespace LightningQueues.Net.Protocol.V1;

public static class Constants
{
    private const string Received = "Received";
    private const string SerializationFailure = "FailDesr";
    private const string ProcessingFailure = "FailPrcs";
    private const string Acknowledged = "Acknowledged";
    private const string QueueDoesNotExist = "Qu-Exist";
        
    // Pre-allocated byte arrays to avoid repeated allocations
    public static readonly byte[] ReceivedBuffer = Encoding.Unicode.GetBytes(Received);
    public static readonly byte[] AcknowledgedBuffer = Encoding.Unicode.GetBytes(Acknowledged);
    public static readonly byte[] QueueDoesNotExistBuffer = Encoding.Unicode.GetBytes(QueueDoesNotExist);
    public static readonly byte[] SerializationFailureBuffer = Encoding.Unicode.GetBytes(SerializationFailure);
    public static readonly byte[] ProcessingFailureBuffer = Encoding.Unicode.GetBytes(ProcessingFailure);
    
    // Pre-allocated ReadOnlyMemory versions for better performance
    public static readonly ReadOnlyMemory<byte> ReceivedMemory = ReceivedBuffer;
    public static readonly ReadOnlyMemory<byte> AcknowledgedMemory = AcknowledgedBuffer;
    public static readonly ReadOnlyMemory<byte> QueueDoesNotExistMemory = QueueDoesNotExistBuffer;
    public static readonly ReadOnlyMemory<byte> SerializationFailureMemory = SerializationFailureBuffer;
    public static readonly ReadOnlyMemory<byte> ProcessingFailureMemory = ProcessingFailureBuffer;
}