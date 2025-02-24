using System.Threading.Tasks;
using Fixie;
using LightningQueues.Tests;

namespace LightningDB.Tests;

class TestProject : ITestProject
{
    public void Configure(TestConfiguration configuration, TestEnvironment environment)
    {
        configuration.Conventions.Add<DefaultDiscovery, ParallelExecutionWithCleanup>();
    }
}

class ParallelExecutionWithCleanup : IExecution
{
    public async Task Run(TestSuite testSuite)
    {
        foreach (var test in testSuite.Tests)
        {
            await test.Run();
        }
        //await Parallel.ForEachAsync(testSuite.Tests, async (test, _) => await test.Run());
        TestBase.CleanupSession();
    }
}