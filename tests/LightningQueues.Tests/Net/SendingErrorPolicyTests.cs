using System;
using System.Reactive.Subjects;
using LightningQueues.Net;
using LightningQueues.Storage;
using Xunit;

namespace LightningQueues.Tests.Net
{
    public class SendingErrorPolicyTests
    {
        private readonly SendingErrorPolicy _errorPolicy;

        public SendingErrorPolicyTests()
        {
            var subject = new Subject<OutgoingMessageFailure>();
            _errorPolicy = new SendingErrorPolicy(new EmptyStore(), subject);
        }

        [Fact]
        public void max_attempts_is_reached()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>();
            message.MaxAttempts = 3;
            _errorPolicy.ShouldRetry(message, 3).ShouldBeFalse();
        }

        [Fact]
        public void max_attempts_is_not_reached()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>();
            message.MaxAttempts = 20;
            _errorPolicy.ShouldRetry(message, 5).ShouldBeTrue();
        }

        [Fact]
        public void deliver_by_has_expired()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>();
            message.DeliverBy = DateTime.Now.Subtract(TimeSpan.FromSeconds(1));
            _errorPolicy.ShouldRetry(message, 5).ShouldBeFalse();
        }

        [Fact]
        public void deliver_by_has_not_expired()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>();
            message.DeliverBy = DateTime.Now.Add(TimeSpan.FromSeconds(1));
            _errorPolicy.ShouldRetry(message, 5).ShouldBeTrue();
        }

        [Fact]
        public void has_neither_deliverby_nor_max_attempts()
        {
            var message = ObjectMother.NewMessage<OutgoingMessage>();
            _errorPolicy.ShouldRetry(message, 5).ShouldBeTrue();
        }

        private class EmptyStore : IMessageStore
        {
            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public ITransaction BeginTransaction()
            {
                throw new NotImplementedException();
            }

            public void CreateQueue(string queueName)
            {
                throw new NotImplementedException();
            }

            public void StoreIncomingMessages(params Message[] messages)
            {
                throw new NotImplementedException();
            }

            public void StoreIncomingMessages(ITransaction transaction, params Message[] messages)
            {
                throw new NotImplementedException();
            }

            public void DeleteIncomingMessages(params Message[] messages)
            {
                throw new NotImplementedException();
            }

            public IObservable<Message> PersistedMessages(string queueName)
            {
                throw new NotImplementedException();
            }

            public IObservable<OutgoingMessage> PersistedOutgoingMessages()
            {
                throw new NotImplementedException();
            }

            public void MoveToQueue(ITransaction transaction, string queueName, Message message)
            {
                throw new NotImplementedException();
            }

            public void SuccessfullyReceived(ITransaction transaction, Message message)
            {
                throw new NotImplementedException();
            }

            public void StoreOutgoing(ITransaction tx, OutgoingMessage message)
            {
                throw new NotImplementedException();
            }

            public int FailedToSend(OutgoingMessage message)
            {
                throw new NotImplementedException();
            }

            public void SuccessfullySent(params OutgoingMessage[] messages)
            {
                throw new NotImplementedException();
            }

            public string[] GetAllQueues()
            {
                throw new NotImplementedException();
            }
        }
    }
}