using BenchmarkDotNet.Attributes;
using LightningQueues.Tests;

namespace LightningQueues.Benchmarks;

[MemoryDiagnoser]
public class SendAndReceive
{
    private Queue _sender;
    private Queue _receiver;
    private OutgoingMessage[] _messages;
    private Task _receivingTask;
    
    [Params(10000)]
    public int MessageCount { get; set; }
    
    [Params(8, 64, 256)]
    public int MessageDataSize { get; set; }
    

    [GlobalSetup]
    public void GlobalSetup()
    {
        var senderPath = Path.Combine(Path.GetTempPath(), "sender", Guid.NewGuid().ToString());
        var receiverPath = Path.Combine(Path.GetTempPath(), "receiver", Guid.NewGuid().ToString());
        _messages = new OutgoingMessage[MessageCount];
        _sender = ObjectMother.NewQueue(path: senderPath, queueName: "sender");
        _receiver = ObjectMother.NewQueue(path: receiverPath, queueName: "receiver");
        _receivingTask = Task.Factory.StartNew(async () =>
        {
            var count = 0;
            await foreach (var message in _receiver.Receive("receiver"))
            {
                Interlocked.Increment(ref count);
                if (count == MessageCount)
                    break;
            }
        });
        var random = new Random();
        for (var i = 0; i < MessageCount; ++i)
        {
            var msg = ObjectMother.NewMessage<OutgoingMessage>("receiver");
            msg.Destination = new Uri($"lq.tcp://{_receiver.Endpoint}");
            msg.Data = new byte[MessageDataSize];
            random.NextBytes(msg.Data);
            _messages[i] = msg;
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _sender.Dispose();
        _receiver.Dispose();
    }

    [Benchmark]
    public async ValueTask Run()
    {
        for (var i = 0; i < MessageCount; ++i)
        {
            _sender.Send(_messages[i]);
        }

        await _receivingTask;
    }
}