using BenchmarkDotNet.Attributes;
using LightningQueues.Builders;
using Microsoft.Extensions.Logging;
using static LightningQueues.Builders.QueueBuilder;

namespace LightningQueues.Benchmarks;

[MemoryDiagnoser]
public class SendAndReceive
{
    private Queue? _sender;
    private Queue? _receiver;
    private Message[]? _messages;
    private Task? _receivingTask;
    
    [Params(10, 100, 1000, 10000)]
    public int MessageCount { get; set; }
    
    [Params(8, 64, 256)]
    public int MessageDataSize { get; set; }
    

    [GlobalSetup]
    public void GlobalSetup()
    {
        var senderPath = Path.Combine(Path.GetTempPath(), "sender", Guid.NewGuid().ToString());
        var receiverPath = Path.Combine(Path.GetTempPath(), "receiver", Guid.NewGuid().ToString());
        _messages = new Message[MessageCount];
        _sender = NewQueue(path: senderPath, queueName: "sender", new RecordingLogger(LogLevel.None));
        _receiver = NewQueue(path: receiverPath, queueName: "receiver", new RecordingLogger(LogLevel.None));
        _receivingTask = Task.Factory.StartNew(async () =>
        {
            var count = 0;
            await foreach (var _ in _receiver.Receive("receiver"))
            {
                Interlocked.Increment(ref count);
                if (count == MessageCount)
                    break;
            }
        });
        var random = new Random();
        for (var i = 0; i < MessageCount; ++i)
        {
            var msg = NewMessage<Message>("receiver");
            msg.Destination = new Uri($"lq.tcp://{_receiver.Endpoint}");
            msg.Data = new byte[MessageDataSize];
            random.NextBytes(msg.Data);
            _messages[i] = msg;
        }
    }

    [GlobalCleanup]
    public async ValueTask GlobalCleanup()
    {
        if (_sender != null && _receiver != null)
        {
            await _sender.DisposeAsync();
            await _receiver.DisposeAsync();
        }
    }

    [Benchmark]
    public async ValueTask Run()
    {
        for (var i = 0; i < MessageCount; ++i)
        {
            _sender?.Send(_messages?[i]);
        }

        if(_receivingTask != null)
            await _receivingTask;
    }
}