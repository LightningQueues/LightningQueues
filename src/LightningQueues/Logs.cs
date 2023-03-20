using System;
using System.Net;
using Microsoft.Extensions.Logging;

namespace LightningQueues;

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
    
    private static readonly Action<ILogger, Uri, Exception> SenderQueueDoesNotExistErrorDefinition = 
        LoggerMessage.Define<Uri>(LogLevel.Error, QueueEvents.Sender, "Queue does not exist at {Uri}");
    public static void SenderQueueDoesNotExistError(this ILogger logger, Uri uri, Exception ex)=>
        SenderQueueDoesNotExistErrorDefinition(logger, uri, ex);
    
    private static readonly Action<ILogger, Uri, Exception> SenderSendingErrorDefinition = 
        LoggerMessage.Define<Uri>(LogLevel.Error, QueueEvents.Sender, "Failed to send messages to {Uri}");
    public static void SenderSendingError(this ILogger logger, Uri uri, Exception ex)=>
        SenderSendingErrorDefinition(logger, uri, ex);
    
    private static readonly Action<ILogger, Exception> SenderSendingLoopErrorDefinition = 
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Sender, "Failed to send messages in message loop");
    public static void SenderSendingLoopError(this ILogger logger, Exception ex)=>
        SenderSendingLoopErrorDefinition(logger, ex);
    
    private static readonly Action<ILogger, Exception> SenderDisposingDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Disposing Sender");
    public static void SenderDisposing(this ILogger logger)=>
        SenderDisposingDefinition(logger, default);
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
    
    private static readonly Action<ILogger, Exception> ReceiverDisposingDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Receiver Disposing");
    public static void ReceiverDisposing(this ILogger logger)=>
        ReceiverDisposingDefinition(logger, default);
    //End Receiver
    
    //Queue
    private static readonly Action<ILogger, MessageId, string, Exception> QueueEnqueueDefinition = 
        LoggerMessage.Define<MessageId, string>(LogLevel.Debug, QueueEvents.Receiver, "Enqueueing message {MessageId} to {QueueName}");
    public static void QueueEnqueue(this ILogger logger, MessageId messageId, string queueName)=>
        QueueEnqueueDefinition(logger, messageId, queueName, default);
    
    private static readonly Action<ILogger, MessageId, TimeSpan, Exception> QueueReceiveLaterDefinition = 
        LoggerMessage.Define<MessageId, TimeSpan>(LogLevel.Debug, QueueEvents.Receiver, "Delaying message {MessageId} until {DelayTime}");
    public static void QueueReceiveLater(this ILogger logger, MessageId messageId, TimeSpan timeSpan)=>
        QueueReceiveLaterDefinition(logger, messageId, timeSpan, default);
    
    private static readonly Action<ILogger, MessageId, TimeSpan, Exception> QueueErrorReceiveLaterDefinition = 
        LoggerMessage.Define<MessageId, TimeSpan>(LogLevel.Error, QueueEvents.Receiver, "Error delaying message {MessageId} until {DelayTime}");
    public static void QueueErrorReceiveLater(this ILogger logger, MessageId messageId, TimeSpan timeSpan, Exception ex)=>
        QueueErrorReceiveLaterDefinition(logger, messageId, timeSpan, ex);
    
    private static readonly Action<ILogger, int, Exception> QueueSendBatchDefinition = 
        LoggerMessage.Define<int>(LogLevel.Debug, QueueEvents.Receiver, "Sending {MessageCount} messages");
    public static void QueueSendBatch(this ILogger logger, int count)=>
        QueueSendBatchDefinition(logger, count, default);
    
    private static readonly Action<ILogger, MessageId, Exception> QueueSendDefinition = 
        LoggerMessage.Define<MessageId>(LogLevel.Debug, QueueEvents.Receiver, "Sending {MessageId}");
    public static void QueueSend(this ILogger logger, MessageId id)=>
        QueueSendDefinition(logger, id, default);
    
    private static readonly Action<ILogger, MessageId, Exception> QueueErrorSendDefinition = 
        LoggerMessage.Define<MessageId>(LogLevel.Error, QueueEvents.Receiver, "Error sending {MessageId}");
    public static void QueueSendError(this ILogger logger, MessageId id, Exception ex)=>
        QueueErrorSendDefinition(logger, id, ex);
    
    private static readonly Action<ILogger, Exception> QueueDisposeDefinition = 
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Disposing queue");
    public static void QueueDispose(this ILogger logger)=>
        QueueDisposeDefinition(logger, default);
    
    private static readonly Action<ILogger, Exception> QueueErrorDisposeDefinition = 
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Receiver, "Error disposing queue");
    public static void QueueDisposeError(this ILogger logger, Exception ex)=>
        QueueErrorDisposeDefinition(logger, ex);
    
    private static readonly Action<ILogger, string, Exception> QueueStartingReceiveDefinition = 
        LoggerMessage.Define<string>(LogLevel.Debug, QueueEvents.Receiver, "Start receiving for {Queue}");
    public static void QueueStartReceiving(this ILogger logger, string queueName)=>
        QueueStartingReceiveDefinition(logger, queueName, default);
    //End Queue
}