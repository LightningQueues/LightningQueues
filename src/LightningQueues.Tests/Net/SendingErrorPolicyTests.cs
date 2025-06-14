﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Logging;
using LightningQueues.Net;
using LightningQueues.Serialization;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using Shouldly;

namespace LightningQueues.Tests.Net;

public class SendingErrorPolicyTests : TestBase
{
    public void max_attempts_is_reached()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(maxAttempts: 3).WithSentAttempts(3);
            policy.ShouldRetry(message).ShouldBeFalse();
        });
    }

    public void max_attempts_is_not_reached()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(maxAttempts: 20).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    public void deliver_by_has_expired()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(deliverBy: DateTime.Now.Subtract(TimeSpan.FromSeconds(1))).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeFalse();
        });
    }

    public void deliver_by_has_not_expired()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = Message.Create(deliverBy: DateTime.Now.Add(TimeSpan.FromSeconds(1))).WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    public void has_neither_deliver_by_nor_max_attempts()
    {
        ErrorPolicyScenario((policy, _, _) =>
        {
            var message = NewMessage().WithSentAttempts(5);
            policy.ShouldRetry(message).ShouldBeTrue();
        });
    }

    public Task message_is_observed_after_time()
    {
        return ErrorPolicyScenario(async (policy, store, failures, cancellation) =>
        {
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 2
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var errorTask = policy.StartRetries(cancellation.Token);
            var failure = new OutgoingMessageFailure
            {
                Messages = [message],
                ShouldRetry = true
            };
            var retryTask = policy.Retries.ReadAllAsync(cancellation.Token)
                .FirstAsync(cancellation.Token);
            failures.Writer.TryWrite(failure);
            var retryMessage = await retryTask;
            retryMessage.Id.ShouldBe(message.Id);
            await cancellation.CancelAsync();
            await DeterministicDelay(50, CancellationToken.None);
            errorTask.IsCanceled.ShouldBeTrue();
        });
    }

    public Task message_removed_from_storage_after_max()
    {
        return ErrorPolicyScenario(async (policy, store, failures, cancellation) =>
        {
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 1
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var failure = new OutgoingMessageFailure
            {
                Messages = [message]
            };
            var errorTask = policy.StartRetries(cancellation.Token);
            var retryTask = policy.Retries.ReadAllAsync(cancellation.Token).FirstAsync(cancellation.Token);
            failures.Writer.TryWrite(failure);
            await DeterministicDelay(TimeSpan.FromSeconds(1), cancellation.Token);
            retryTask.IsCompleted.ShouldBeFalse();
            store.PersistedOutgoing().Any().ShouldBeFalse();
            await cancellation.CancelAsync();
            await DeterministicDelay(50, CancellationToken.None);
            errorTask.IsCanceled.ShouldBeTrue();
        });
    }

    public Task time_increases_with_each_failure()
    {
        return ErrorPolicyScenario(async (policy, store, failures, _) =>
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(6));
            Message? observed = null;
            var message = Message.Create(
                destinationUri: "lq.tcp://localhost:5150/blah",
                maxAttempts: 5
            );
            using (var tx = store.BeginTransaction())
            {
                store.StoreOutgoing(tx, message);
                tx.Commit();
            }

            var errorTask = policy.StartRetries(cancellation.Token);
            var failure = new OutgoingMessageFailure
            {
                Messages = [message],
                ShouldRetry = true
            };
            var retriesTask = Task.Factory.StartNew(async () =>
            {
                await foreach (var msg in policy.Retries.ReadAllAsync(cancellation.Token))
                {
                    observed = msg;
                }
            }, cancellation.Token);
            failures.Writer.TryWrite(failure);
            await DeterministicDelay(TimeSpan.FromSeconds(1.5), cancellation.Token);
            observed.ShouldNotBeNull("first");
            observed = null;
            failures.Writer.TryWrite(failure);
            observed.ShouldBeNull("second");
            await DeterministicDelay(TimeSpan.FromSeconds(1), cancellation.Token);
            observed.ShouldBeNull("third");
            await Task.WhenAny(DeterministicDelay(TimeSpan.FromSeconds(4), cancellation.Token));
            observed.ShouldNotBeNull("fourth");
            await cancellation.CancelAsync();
            await Task.WhenAny(errorTask.AsTask(), retriesTask);
        });
    }

    public async Task errors_in_storage_dont_end_stream()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var message = NewMessage();
        var store = new StubMessageStore();
        var errorPolicy = new SendingErrorPolicy(new RecordingLogger(Console), store, failures);
        var ended = false;
        var failure = new OutgoingMessageFailure
        {
            Messages = [message]
        };
        var retryTask = Task.Factory.StartNew(async () =>
        {
            await foreach (var _ in errorPolicy.Retries.ReadAllAsync(cancellation.Token))
            {
            }
            ended = true;
        }, cancellation.Token);
        failures.Writer.TryWrite(failure);
        await Task.WhenAny(retryTask, DeterministicDelay(TimeSpan.FromSeconds(1), cancellation.Token));
        ended.ShouldBeFalse();
        await cancellation.CancelAsync();
    }

    private void ErrorPolicyScenario(Action<SendingErrorPolicy, IMessageStore, Channel<OutgoingMessageFailure>> scenario)
    {
        var logger = new RecordingLogger(Console);
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var errorPolicy = new SendingErrorPolicy(logger, store, failures);
        scenario(errorPolicy, store, failures);
    }

    private async Task ErrorPolicyScenario(
        Func<SendingErrorPolicy, IMessageStore, Channel<OutgoingMessageFailure>, CancellationTokenSource, Task> scenario)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var logger = new RecordingLogger(Console);
        using var env = LightningEnvironment();
        using var store = new LmdbMessageStore(env, new MessageSerializer());
        var failures = Channel.CreateUnbounded<OutgoingMessageFailure>();
        var errorPolicy = new SendingErrorPolicy(logger, store, failures);
        await scenario(errorPolicy, store, failures, cancellation);
        await cancellation.CancelAsync();
    }
}

public class StubMessageStore : IMessageStore
{
    public void Dispose()
    {
    }

    public LmdbTransaction BeginTransaction()
    {
        throw new NotImplementedException();
    }

    public void CreateQueue(string queueName)
    {
        throw new NotImplementedException();
    }

    public void StoreIncoming(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void StoreIncoming(LmdbTransaction transaction, params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void DeleteIncoming(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<Message> PersistedIncoming(string queueName)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<Message> PersistedOutgoing()
    {
        throw new NotImplementedException();
    }

    public void MoveToQueue(LmdbTransaction transaction, string queueName, Message message)
    {
        throw new NotImplementedException();
    }

    public void SuccessfullyReceived(LmdbTransaction transaction, Message message)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(LmdbTransaction tx, Message message)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(Message message)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void StoreOutgoing(ReadOnlySpan<Message> messages)
    {
        throw new NotImplementedException();
    }

    public void FailedToSend(bool shouldRemove = false, params IEnumerable<Message> message)
    {
        throw new NotImplementedException();
    }

    public void SuccessfullySent(params IEnumerable<Message> messages)
    {
        throw new NotImplementedException();
    }

    public Message? GetMessage(string queueName, MessageId messageId)
    {
        throw new NotImplementedException();
    }

    public string[] GetAllQueues()
    {
        throw new NotImplementedException();
    }

    public void ClearAllStorage()
    {
        throw new NotImplementedException();
    }
}