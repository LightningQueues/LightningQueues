using System;
using System.Net;
using Microsoft.Extensions.Logging;

namespace LightningQueues.Net;

internal static class Logs
{
    //Sender
    private static readonly Action<ILogger, Exception> SenderWritingMessageBatchDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Writing message batch to destination");
    public static void SenderWritingMessageBatch(this ILogger logger)=>
        SenderWritingMessageBatchDefinition(logger, default);
    private static readonly Action<ILogger, Exception> SenderSuccessfullyWroteMessageBatchDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully wrote message batch to destination");
    public static void SenderSuccessfullyWroteMessageBatch(this ILogger logger)=>
        SenderSuccessfullyWroteMessageBatchDefinition(logger, default);
    
    private static readonly Action<ILogger, Exception> SenderSuccessfullyReadReceivedDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully read received message");
    public static void SenderSuccessfullyReadReceived(this ILogger logger)=>
        SenderSuccessfullyReadReceivedDefinition(logger, default);
    
    private static readonly Action<ILogger, Exception> SenderSuccessfullyWroteAcknowledgementDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully wrote acknowledgement");
    public static void SenderSuccessfullyWroteAcknowledgement(this ILogger logger)=>
        SenderSuccessfullyWroteAcknowledgementDefinition(logger, default);
    
    private static readonly Action<ILogger, Exception> SenderStorageSuccessfullySentDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Stored that messages were successful");
    public static void SenderStorageSuccessfullySent(this ILogger logger)=>
        SenderStorageSuccessfullySentDefinition(logger, default);
    //End Sender

    //Receiver
    private static readonly Action<ILogger, int, Exception> ReceiverReceivedLengthDefinition = 
        LoggerMessage.Define<int>(LogLevel.Debug, QueueEvents.Receiver, "Received length value of {Length}");
    public static void ReceiverReceivedLength(this ILogger logger, int length)=>
        ReceiverReceivedLengthDefinition(logger, length, default);
    private static readonly Action<ILogger, EndPoint, Exception> ReceiverErrorReceivingMessagesDefinition = 
        LoggerMessage.Define<EndPoint>(LogLevel.Debug, QueueEvents.Receiver, "Error reading messages from {Endpoint}");
    public static void ReceiverErrorReadingMessages(this ILogger logger, EndPoint endPoint, Exception ex)=>
        ReceiverErrorReceivingMessagesDefinition(logger, endPoint, ex);
    //End Receiver
}