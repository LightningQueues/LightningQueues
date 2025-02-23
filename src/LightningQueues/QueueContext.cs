using System;
using System.Collections.Generic;
using LightningQueues.Storage;
using LightningQueues.Storage.LMDB;

namespace LightningQueues;

internal class QueueContext : IQueueContext
{
    private readonly Queue _queue;
    private readonly Message _message;
    private readonly List<IQueueAction> _queueActions;

    internal QueueContext(Queue queue, Message message)
    {
        _queue = queue;
        _message = message;
        _queueActions = new List<IQueueAction>();
    }

    public void CommitChanges()
    {
        using var transaction = _queue.Store.BeginTransaction();
        foreach (var action in _queueActions)
        {
            action.Execute(transaction);
        }
        transaction.Commit();

        foreach (var action in _queueActions)
        {
            action.Success();
        }
    }

    public void Send(Message message)
    {
        _queueActions.Add(new SendAction(this, message));
    }

    public void ReceiveLater(TimeSpan timeSpan)
    {
        _queueActions.Add(new ReceiveLaterTimeSpanAction(this, timeSpan));
    }

    public void ReceiveLater(DateTimeOffset time)
    {
        _queueActions.Add(new ReceiveLaterDateTimeOffsetAction(this, time));
    }

    public void SuccessfullyReceived()
    {
        _queueActions.Add(new SuccessAction(this));
    }

    public void MoveTo(string queueName)
    {
        _queueActions.Add(new MoveAction(this, queueName));
    }

    public void Enqueue(Message message)
    {
        _queueActions.Add(new EnqueueAction(this, message));
    }

    private interface IQueueAction
    {
        void Execute(LmdbTransaction transaction);
        void Success();
    }

    private class SendAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _message;

        public SendAction(QueueContext context, Message message)
        {
            _context = context;
            _message = message;
        }

        public void Execute(LmdbTransaction transaction)
        {
            _context._queue.Store.StoreOutgoing(transaction, _message);
        }

        public void Success()
        {
            _context._queue.SendingChannel.TryWrite(_message);
        }
    }

    private class EnqueueAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly Message _message;

        public EnqueueAction(QueueContext context, Message message)
        {
            _context = context;
            _message = message;
        }

        public void Execute(LmdbTransaction transaction)
        {
            _context._queue.Store.StoreIncoming(transaction, _message);
        }

        public void Success()
        {
            _context._queue.ReceivingChannel.TryWrite(_message);
        }
    }

    private class MoveAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly string _queueName;

        public MoveAction(QueueContext context, string queueName)
        {
            _context = context;
            _queueName = queueName;
        }

        public void Execute(LmdbTransaction transaction)
        {
            _context._queue.Store.MoveToQueue(transaction, _queueName, _context._message);
        }

        public void Success()
        {
            var message = _context._message;
            message.Queue = _queueName;
            _context._queue.ReceivingChannel.TryWrite(message);
        }
    }

    private class SuccessAction : IQueueAction
    {
        private readonly QueueContext _context;

        public SuccessAction(QueueContext context)
        {
            _context = context;
        }

        public void Execute(LmdbTransaction transaction)
        {
            _context._queue.Store.SuccessfullyReceived(transaction, _context._message);
        }

        public void Success()
        {
        }
    }

    private class ReceiveLaterTimeSpanAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly TimeSpan _timeSpan;

        public ReceiveLaterTimeSpanAction(QueueContext context, TimeSpan timeSpan)
        {
            _context = context;
            _timeSpan = timeSpan;
        }

        public void Execute(LmdbTransaction transaction)
        {
        }

        public void Success()
        {
            _context._queue.ReceiveLater(_context._message, _timeSpan);
        }
    }

    private class ReceiveLaterDateTimeOffsetAction : IQueueAction
    {
        private readonly QueueContext _context;
        private readonly DateTimeOffset _time;

        public ReceiveLaterDateTimeOffsetAction(QueueContext context, DateTimeOffset time)
        {
            _context = context;
            _time = time;
        }


        public void Execute(LmdbTransaction transaction)
        {
        }

        public void Success()
        {
            _context._queue.ReceiveLater(_context._message, _time);
        }
    }
}