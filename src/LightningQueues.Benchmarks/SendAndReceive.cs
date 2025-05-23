using BenchmarkDotNet.Attributes;
using LightningDB;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;

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
        _sender = new QueueConfiguration()
            .WithDefaults()
            .StoreWithLmdb(senderPath, new EnvironmentConfiguration { MapSize = 1024 * 1024 * 100, MaxDatabases = 5 })
            .BuildQueue();
        _sender.CreateQueue("sender");
        _receiver = new QueueConfiguration()
            .WithDefaults()
            .StoreWithLmdb(receiverPath, new EnvironmentConfiguration { MapSize = 1024 * 1024 * 100, MaxDatabases = 5 })
            .BuildQueue();
        _sender.CreateQueue("receiver");
        _sender.Start();
        _receiver.Start();
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
            var data = new byte[MessageDataSize];
            random.NextBytes(data);
            var msg = Message.Create(
                data: data,
                queue: "receiver",
                destinationUri: $"lq.tcp://{_receiver.Endpoint}"
            );
            _messages[i] = msg;
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (_sender != null && _receiver != null)
        {
            using(_sender)
            using (_receiver)
            {
            }
        }
    }

    [Benchmark]
    public async ValueTask Run()
    {
        for (var i = 0; i < MessageCount; ++i)
        {
            _sender?.Send(_messages![i]);
        }

        if(_receivingTask != null)
            await _receivingTask;
    }
}