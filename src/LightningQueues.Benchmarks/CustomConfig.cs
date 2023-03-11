using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Validators;

namespace LightningQueues.Benchmarks;

public class CustomConfig : ManualConfig
{
    public CustomConfig()
    {
        AddValidator(JitOptimizationsValidator.DontFailOnError);
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
    }
}