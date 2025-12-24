using BenchmarkDotNet.Running;
using LightningQueues.Benchmarks;

var switcher = new BenchmarkSwitcher([
    typeof(SendAndReceive),
    typeof(LmdbStorageBenchmark)
]);

switcher.Run(args, new CustomConfig());