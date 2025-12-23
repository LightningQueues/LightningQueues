using System;
using System.Net;
using Microsoft.Extensions.Logging;

namespace LightningQueues;

internal static class Logs
{
    //Sender
    private static readonly Action<ILogger, Exception?> SenderWritingMessageBatchDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Writing message batch to destination");
    public static void SenderWritingMessageBatch(this ILogger logger) =>
        SenderWritingMessageBatchDefinition(logger, null);
    private static readonly Action<ILogger, Exception?> SenderSuccessfullyWroteMessageBatchDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully wrote message batch to destination");
    public static void SenderSuccessfullyWroteMessageBatch(this ILogger logger) =>
        SenderSuccessfullyWroteMessageBatchDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> SenderSuccessfullyReadReceivedDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully read received message");
    public static void SenderSuccessfullyReadReceived(this ILogger logger) =>
        SenderSuccessfullyReadReceivedDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> SenderSuccessfullyWroteAcknowledgementDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Successfully wrote acknowledgement");
    public static void SenderSuccessfullyWroteAcknowledgement(this ILogger logger) =>
        SenderSuccessfullyWroteAcknowledgementDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> SenderStorageSuccessfullySentDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Stored that messages were successful");
    public static void SenderStorageSuccessfullySent(this ILogger logger) =>
        SenderStorageSuccessfullySentDefinition(logger, null);
    
    private static readonly Action<ILogger, Uri, Exception?> SenderQueueDoesNotExistErrorDefinition =
        LoggerMessage.Define<Uri>(LogLevel.Error, QueueEvents.Sender, "Queue does not exist at {Uri}");
    public static void SenderQueueDoesNotExistError(this ILogger logger, Uri uri, Exception ex) =>
        SenderQueueDoesNotExistErrorDefinition(logger, uri, ex);

    private static readonly Action<ILogger, Uri, Exception?> SenderSendingErrorDefinition =
        LoggerMessage.Define<Uri>(LogLevel.Error, QueueEvents.Sender, "Failed to send messages to {Uri}");
    public static void SenderSendingError(this ILogger logger, Uri uri, Exception ex) =>
        SenderSendingErrorDefinition(logger, uri, ex);

    private static readonly Action<ILogger, Exception?> SenderSendingLoopErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Sender, "Failed to send messages in message loop");
    public static void SenderSendingLoopError(this ILogger logger, Exception ex) =>
        SenderSendingLoopErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> SenderDisposingDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Disposing Sender");
    public static void SenderDisposing(this ILogger logger) =>
        SenderDisposingDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> SenderDisposalErrorDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Sender, "Error during sender disposal");
    public static void SenderDisposalError(this ILogger logger, Exception ex) =>
        SenderDisposalErrorDefinition(logger, ex);
    //End Sender

    //Receiver
    private static readonly Action<ILogger, int, Exception?> ReceiverReceivedLengthDefinition =
        LoggerMessage.Define<int>(LogLevel.Debug, QueueEvents.Receiver, "Received length value of {Length}");
    public static void ReceiverReceivedLength(this ILogger logger, int length) =>
        ReceiverReceivedLengthDefinition(logger, length, null);

    private static readonly Action<ILogger, EndPoint, Exception?> ReceiverErrorReceivingMessagesDefinition =
        LoggerMessage.Define<EndPoint>(LogLevel.Debug, QueueEvents.Receiver, "Error reading messages from {Endpoint}");
    public static void ReceiverErrorReadingMessages(this ILogger logger, EndPoint endPoint, Exception ex) =>
        ReceiverErrorReceivingMessagesDefinition(logger, endPoint, ex);

    private static readonly Action<ILogger, Exception?> ReceiverDisposingDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Receiver Disposing");
    public static void ReceiverDisposing(this ILogger logger) =>
        ReceiverDisposingDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> ReceiverAcceptErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Receiver, "Error accepting socket");
    public static void ReceiverAcceptError(this ILogger logger, Exception ex) =>
        ReceiverAcceptErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> ReceiverDisposalErrorDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Error stopping listener during disposal");
    public static void ReceiverDisposalError(this ILogger logger, Exception ex) =>
        ReceiverDisposalErrorDefinition(logger, ex);
    //End Receiver
    
    //Queue
    private static readonly Action<ILogger, MessageId, string?, Exception?> QueueEnqueueDefinition =
        LoggerMessage.Define<MessageId, string?>(LogLevel.Debug, QueueEvents.Receiver, "Enqueueing message {MessageId} to {QueueName}");
    public static void QueueEnqueue(this ILogger logger, MessageId messageId, string? queueName) =>
        QueueEnqueueDefinition(logger, messageId, queueName, null);

    private static readonly Action<ILogger, MessageId, TimeSpan, Exception?> QueueReceiveLaterDefinition =
        LoggerMessage.Define<MessageId, TimeSpan>(LogLevel.Debug, QueueEvents.Receiver, "Delaying message {MessageId} until {DelayTime}");
    public static void QueueReceiveLater(this ILogger logger, MessageId messageId, TimeSpan timeSpan) =>
        QueueReceiveLaterDefinition(logger, messageId, timeSpan, null);

    private static readonly Action<ILogger, MessageId, TimeSpan, Exception?> QueueErrorReceiveLaterDefinition =
        LoggerMessage.Define<MessageId, TimeSpan>(LogLevel.Error, QueueEvents.Receiver, "Error delaying message {MessageId} until {DelayTime}");
    public static void QueueErrorReceiveLater(this ILogger logger, MessageId messageId, TimeSpan timeSpan, Exception ex) =>
        QueueErrorReceiveLaterDefinition(logger, messageId, timeSpan, ex);

    private static readonly Action<ILogger, int, Exception?> QueueSendBatchDefinition =
        LoggerMessage.Define<int>(LogLevel.Debug, QueueEvents.Receiver, "Sending {MessageCount} messages");
    public static void QueueSendBatch(this ILogger logger, int count) =>
        QueueSendBatchDefinition(logger, count, null);

    private static readonly Action<ILogger, MessageId, Exception?> QueueSendDefinition =
        LoggerMessage.Define<MessageId>(LogLevel.Debug, QueueEvents.Receiver, "Sending {MessageId}");
    public static void QueueSend(this ILogger logger, MessageId id) =>
        QueueSendDefinition(logger, id, null);

    private static readonly Action<ILogger, MessageId, Exception?> QueueErrorSendDefinition =
        LoggerMessage.Define<MessageId>(LogLevel.Error, QueueEvents.Receiver, "Error sending {MessageId}");
    public static void QueueSendError(this ILogger logger, MessageId id, Exception ex) =>
        QueueErrorSendDefinition(logger, id, ex);

    private static readonly Action<ILogger, Exception?> QueueDisposeDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Disposing queue");
    public static void QueueDispose(this ILogger logger) =>
        QueueDisposeDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> QueueErrorDisposeDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Receiver, "Error disposing queue");
    public static void QueueDisposeError(this ILogger logger, Exception ex) =>
        QueueErrorDisposeDefinition(logger, ex);

    private static readonly Action<ILogger, string, Exception?> QueueStartingReceiveDefinition =
        LoggerMessage.Define<string>(LogLevel.Debug, QueueEvents.Receiver, "Start receiving for {Queue}");
    public static void QueueStartReceiving(this ILogger logger, string queueName) =>
        QueueStartingReceiveDefinition(logger, queueName, null);

    private static readonly Action<ILogger, Exception?> QueueStartErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Receiver, "Error starting queue");
    public static void QueueStartError(this ILogger logger, Exception ex) =>
        QueueStartErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> QueueStartingDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Starting LightningQueues");
    public static void QueueStarting(this ILogger logger) =>
        QueueStartingDefinition(logger, null);

    private static readonly Action<ILogger, MessageId, string, Exception?> QueueMoveMessageDefinition =
        LoggerMessage.Define<MessageId, string>(LogLevel.Debug, QueueEvents.Receiver, "Moving message {MessageIdentifier} to {QueueName}");
    public static void QueueMoveMessage(this ILogger logger, MessageId messageId, string queueName) =>
        QueueMoveMessageDefinition(logger, messageId, queueName, null);

    private static readonly Action<ILogger, Exception?> QueueOutgoingErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Receiver, "Error sending queue outgoing messages");
    public static void QueueOutgoingError(this ILogger logger, Exception ex) =>
        QueueOutgoingErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> QueueTasksTimeoutDefinition =
        LoggerMessage.Define(LogLevel.Warning, QueueEvents.Receiver, "Tasks did not complete within timeout during disposal");
    public static void QueueTasksTimeout(this ILogger logger) =>
        QueueTasksTimeoutDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> QueueTasksDisposeExceptionDefinition =
        LoggerMessage.Define(LogLevel.Debug, QueueEvents.Receiver, "Exception waiting for tasks to complete during disposal");
    public static void QueueTasksDisposeException(this ILogger logger, Exception ex) =>
        QueueTasksDisposeExceptionDefinition(logger, ex);
    //End Queue
    
    //SendingErrorPolicy
    private static readonly Action<ILogger, int, int, Exception?> PolicyShouldRetryAttemptsDefinition =
        LoggerMessage.Define<int, int>(LogLevel.Debug, QueueEvents.Queue, "Failed to send should retry with on: {AttemptCount}, out of {TotalAttempts}");
    public static void PolicyShouldRetryAttempts(this ILogger logger, int attempts, int totalAttempts) =>
        PolicyShouldRetryAttemptsDefinition(logger, attempts, totalAttempts, null);

    private static readonly Action<ILogger, DateTime?, DateTime, Exception?> PolicyShouldRetryTimingDefinition =
        LoggerMessage.Define<DateTime?, DateTime>(LogLevel.Debug, QueueEvents.Queue, "Failed to send should retry with: {DeliverBy}, due to {CurrentTime}");
    public static void PolicyShouldRetryTiming(this ILogger logger, DateTime? deliverBy, DateTime currentTime) =>
        PolicyShouldRetryTimingDefinition(logger, deliverBy, currentTime, null);

    private static readonly Action<ILogger, Exception?> PolicyIncrementFailureErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Queue, "Failed to increment send failure");
    public static void PolicyIncrementFailureError(this ILogger logger, Exception ex) =>
        PolicyIncrementFailureErrorDefinition(logger, ex);
    //End SendingErrorPolicy

    //Protocol
    private static readonly Action<ILogger, Exception?> ProtocolReadErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Protocol, "Failed to read from client, possible disconnected client or malformed request");
    public static void ProtocolReadError(this ILogger logger) =>
        ProtocolReadErrorDefinition(logger, null);

    private static readonly Action<ILogger, Exception?> ProtocolReadMessagesErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Protocol, "Failed to read messages");
    public static void ProtocolReadMessagesError(this ILogger logger, Exception ex) =>
        ProtocolReadMessagesErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> ProtocolQueueNotFoundErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Protocol, "Failed to send queue not found");
    public static void ProtocolQueueNotFoundError(this ILogger logger, Exception ex) =>
        ProtocolQueueNotFoundErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> ProtocolProcessingErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Protocol, "Failed to send processing error");
    public static void ProtocolProcessingError(this ILogger logger, Exception ex) =>
        ProtocolProcessingErrorDefinition(logger, ex);

    private static readonly Action<ILogger, Exception?> ProtocolStreamErrorDefinition =
        LoggerMessage.Define(LogLevel.Error, QueueEvents.Protocol, "Error reading from stream");
    public static void ProtocolStreamError(this ILogger logger, Exception ex) =>
        ProtocolStreamErrorDefinition(logger, ex);
    //End Protocol
}