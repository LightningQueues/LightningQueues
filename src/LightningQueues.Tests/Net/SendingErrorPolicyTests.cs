using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using LightningQueues.Builders;
using Microsoft.Extensions.Logging;
using LightningQueues.Net;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests.Net;

[Collection("SharedTestDirectory")]
public class SendingErrorPolicyTests : IDisposable
{
    private readonly SendingErrorPolicy _errorPolicy;
    private readonly LmdbMessageStore _store;
    private readonly Channel<OutgoingMessageFailure> _failureChannel;

    public SendingErrorPolicyTests(SharedTestDirectory testDirectory)
    {
        ILogger logger = new RecordingLogger();
        _store = new LmdbMessageStore(testDirectory.CreateNewDirectoryForTest());
        _failureChannel = Channel.CreateUnbounded<OutgoingMessageFailure>();
        _errorPolicy = new SendingErrorPolicy(logger, _store, _failureChannel);
    }

    [Fact]
    public void max_attempts_is_reached()
    {
        var message = NewMessage<OutgoingMessage>();
        message.MaxAttempts = 3;
        message.SentAttempts = 3;
        _errorPolicy.ShouldRetry(message).ShouldBeFalse();
    }

    [Fact]
    public void max_attempts_is_not_reached()
    {
        var message = NewMessage<OutgoingMessage>();
        message.MaxAttempts = 20;
        message.SentAttempts = 5;
        _errorPolicy.ShouldRetry(message).ShouldBeTrue();
    }

    [Fact]
    public void deliver_by_has_expired()
    {
        var message = NewMessage<OutgoingMessage>();
        message.DeliverBy = DateTime.Now.Subtract(TimeSpan.FromSeconds(1));
        message.SentAttempts = 5;
        _errorPolicy.ShouldRetry(message).ShouldBeFalse();
    }

    [Fact]
    public void deliver_by_has_not_expired()
    {
        var message = NewMessage<OutgoingMessage>();
        message.DeliverBy = DateTime.Now.Add(TimeSpan.FromSeconds(1));
        message.SentAttempts = 5;
        _errorPolicy.ShouldRetry(message).ShouldBeTrue();
    }

    [Fact]
    public void has_neither_deliver_by_nor_max_attempts()
    {
        var message = NewMessage<OutgoingMessage>();
        message.SentAttempts = 5;
        _errorPolicy.ShouldRetry(message).ShouldBeTrue();
    }

    [Fact]
    public async ValueTask message_is_observed_after_time()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var message = NewMessage<OutgoingMessage>();
        message.Destination = new Uri("lq.tcp://localhost:5150/blah");
        message.MaxAttempts = 2;
        var tx = _store.BeginTransaction();
        _store.StoreOutgoing(tx, message);
        tx.Commit();
        var failure = new OutgoingMessageFailure
        {
            Messages = new [] { message }
        };
        var retryTask = _errorPolicy.Retries.ReadAllAsync(cancellation.Token).FirstAsync(cancellation.Token);
        _failureChannel.Writer.TryWrite(failure);
        await Task.Delay(TimeSpan.FromSeconds(1), cancellation.Token);
        retryTask.IsCompleted.ShouldBeTrue();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask message_removed_from_storage_after_max()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var message = NewMessage<OutgoingMessage>();
        message.Destination = new Uri("lq.tcp://localhost:5150/blah");
        message.MaxAttempts = 1;
        var tx = _store.BeginTransaction();
        _store.StoreOutgoing(tx, message);
        tx.Commit();
        var failure = new OutgoingMessageFailure
        {
            Messages = new [] { message }
        };
        var retryTask = _errorPolicy.Retries.ReadAllAsync(cancellation.Token).FirstAsync(cancellation.Token);
        _failureChannel.Writer.TryWrite(failure);
        await Task.Delay(TimeSpan.FromSeconds(1), cancellation.Token);
        retryTask.IsCompleted.ShouldBeFalse();
        _store.PersistedOutgoingMessages().Any().ShouldBeFalse();
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask time_increases_with_each_failure()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        Message observed = null;
        var message = NewMessage<OutgoingMessage>();
        message.Destination = new Uri("lq.tcp://localhost:5150/blah");
        message.MaxAttempts = 5;
        var tx = _store.BeginTransaction();
        _store.StoreOutgoing(tx, message);
        tx.Commit();
        var failure = new OutgoingMessageFailure
        {
            Messages = new [] { message }
        };
        var retryTask = Task.Factory.StartNew(async () =>
        {
            await foreach (var msg in _errorPolicy.Retries.ReadAllAsync(cancellation.Token))
            {
                observed = msg;
            }
        }, cancellation.Token);
        _failureChannel.Writer.TryWrite(failure);
        await Task.Delay(TimeSpan.FromSeconds(1), cancellation.Token);
        observed.ShouldNotBeNull("first");
        observed = null;
        _failureChannel.Writer.TryWrite(failure);
        observed.ShouldBeNull("second");
        await Task.Delay(TimeSpan.FromSeconds(1), cancellation.Token);
        observed.ShouldBeNull("third");
        await Task.WhenAny(Task.Delay(TimeSpan.FromSeconds(3), cancellation.Token), retryTask);
        observed.ShouldNotBeNull("fourth");
        cancellation.Cancel();
    }

    [Fact]
    public async ValueTask errors_in_storage_dont_end_stream()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var message = NewMessage<OutgoingMessage>();
        var store = Substitute.For<IMessageStore>();
        store.FailedToSend(Arg.Is(message)).Throws(new Exception("bam!"));
        var errorPolicy = new SendingErrorPolicy(new RecordingLogger(), store, _failureChannel);
        var ended = false;
        var failure = new OutgoingMessageFailure
        {
            Messages = new [] { message }
        };
        var retryTask = Task.Factory.StartNew(async () =>
        {
            await foreach (var _ in errorPolicy.Retries.ReadAllAsync(cancellation.Token))
            {
            }
            ended = true;
        }, cancellation.Token);
        _failureChannel.Writer.TryWrite(failure);
        await Task.WhenAny(retryTask, Task.Delay(TimeSpan.FromSeconds(1), cancellation.Token));
        ended.ShouldBeFalse();
        cancellation.Cancel();
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}