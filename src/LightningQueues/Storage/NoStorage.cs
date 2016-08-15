using System;
using System.Collections.Generic;
using System.Reactive.Linq;

namespace LightningQueues.Storage
{
    public class NoStorage : IMessageStore
    {
        private const string SentAttempts = "SENT_ATTEMPTS";
        private readonly List<string> _queues = new List<string>();

        private class NoStorageTransaction : ITransaction
        {
            public Guid TransactionId => Guid.Empty;

            public void Commit()
            {
            }

            public void Rollback()
            {
            }
        }

        public void Dispose()
        {
        }

        public ITransaction BeginTransaction()
        {
            return new NoStorageTransaction();
        }

        public void CreateQueue(string queueName)
        {
            _queues.Add(queueName);
        }

        public void StoreIncomingMessages(params Message[] messages)
        {
        }

        public void StoreIncomingMessages(ITransaction transaction, params Message[] messages)
        {
        }

        public void DeleteIncomingMessages(params Message[] messages)
        {
        }

        public IObservable<Message> PersistedMessages(string queueName)
        {
            return Observable.Empty<Message>();
        }

        public IObservable<OutgoingMessage> PersistedOutgoingMessages()
        {
            return Observable.Empty<OutgoingMessage>();
        }

        public void MoveToQueue(ITransaction transaction, string queueName, Message message)
        {
            message.Queue = queueName;
        }

        public void SuccessfullyReceived(ITransaction transaction, Message message)
        {
        }

        public void StoreOutgoing(ITransaction tx, OutgoingMessage message)
        {
        }

        public void StoreOutgoing(ITransaction tx, OutgoingMessage[] message)
        {
        }

        public int FailedToSend(OutgoingMessage message)
        {
            var attempts = 0;
            if (message.Headers.ContainsKey(SentAttempts))
            {
                var current = int.Parse(message.Headers[SentAttempts]);
                attempts = current + 1;
            }
            message.Headers[SentAttempts] = attempts.ToString();
            return attempts;
        }

        public void SuccessfullySent(params OutgoingMessage[] messages)
        {
        }

        public Message GetMessage(string queueName, MessageId messageId)
        {
            return null;
        }

        public string[] GetAllQueues()
        {
            return _queues.ToArray();
        }

        public void ClearAllStorage()
        {
        }
    }
}