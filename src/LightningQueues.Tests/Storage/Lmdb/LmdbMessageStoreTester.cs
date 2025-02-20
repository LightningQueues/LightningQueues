﻿using System;
using System.Linq;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;
using Shouldly;
using Xunit;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Tests.Storage.Lmdb;

[Collection("SharedTestDirectory")]
public class LmdbMessageStoreTester : IDisposable
{
    private readonly LmdbMessageStore _store;
    private readonly string _path;

    public LmdbMessageStoreTester(SharedTestDirectory testDirectory)
    {
        _path = testDirectory.CreateNewDirectoryForTest();
        _store = new LmdbMessageStore(_path, new MessageSerializer());
    }

    [Fact]
    public void getting_all_queues()
    {
        _store.CreateQueue("test");
        _store.CreateQueue("test2");
        _store.CreateQueue("test3");
        _store.GetAllQueues().SequenceEqual(new[] {"test", "test2", "test3"}).ShouldBeTrue();
    }

    [Fact]
    public void clear_all_history_with_empty_dataset_doesnt_throw()
    {
        _store.CreateQueue("test");
        _store.ClearAllStorage();
    }

    [Fact]
    public void clear_all_history_with_persistent_data()
    {
        _store.CreateQueue("test");
        var message = NewMessage<Message>("test");
        var outgoingMessage = NewMessage<Message>();
        outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
        outgoingMessage.SentAt = DateTime.Now;
        var tx = _store.BeginTransaction();
        _store.StoreOutgoing(tx, outgoingMessage);
        _store.StoreIncoming(tx, message);
        tx.Commit();
        tx.Dispose();
        _store.PersistedIncoming("test").Count().ShouldBe(1);
        _store.PersistedOutgoing().Count().ShouldBe(1);
        _store.ClearAllStorage();
        _store.PersistedIncoming("test").Count().ShouldBe(0);
        _store.PersistedOutgoing().Count().ShouldBe(0);
    }

    [Fact]
    public void store_can_read_previously_stored_items()
    {
        _store.CreateQueue("test");
        var message = NewMessage<Message>("test");
        var outgoingMessage = NewMessage<Message>();
        outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
        outgoingMessage.SentAt = DateTime.Now;
        using (var tx = _store.BeginTransaction())
        {
            _store.StoreOutgoing(tx, outgoingMessage);
            _store.StoreIncoming(tx, message);
            tx.Commit();
        }
        _store.Dispose();
        using var store2 = new LmdbMessageStore(_path, new MessageSerializer());
        store2.CreateQueue("test");
        store2.PersistedIncoming("test").Count().ShouldBe(1);
        store2.PersistedOutgoing().Count().ShouldBe(1);
    }

    [Fact]
    public void retrieve_message_by_id()
    {
        _store.CreateQueue("test");
        var message = NewMessage<Message>("test");
        var outgoingMessage = NewMessage<Message>();
        outgoingMessage.Destination = new Uri("lq.tcp://localhost:3030");
        outgoingMessage.SentAt = DateTime.Now;
        using (var tx = _store.BeginTransaction())
        {
            _store.StoreOutgoing(tx, outgoingMessage);
            _store.StoreIncoming(tx, message);
            tx.Commit();
        }

        var message2 = _store.GetMessage(message.Queue, message.Id);
        var outgoing2 = _store.GetMessage("outgoing", outgoingMessage.Id);
        message2.ShouldNotBeNull();
        outgoing2.ShouldNotBeNull();
    }

    public void Dispose()
    {
        _store.Dispose();
        GC.SuppressFinalize(this);
    }
}