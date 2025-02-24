using System;
using System.Threading.Tasks;
using Fixie;
using LightningQueues.Tests;

namespace LightningDB.Tests;

class TestProject : ITestProject
{
    public void Configure(TestConfiguration configuration, TestEnvironment environment)
    {
        configuration.Conventions.Add(new DefaultDiscovery(), new ParallelExecutionWithCleanup(environment));
    }
}

class ParallelExecutionWithCleanup(TestEnvironment environment) : IExecution
{
    public async Task Run(TestSuite testSuite)
    {
        foreach (var testClass in testSuite.TestClasses)
        {
            foreach (var test in testClass.Tests)
            {
                var instance = testClass.Construct();
                if (instance is TestBase baseTest)
                {
                    baseTest.Console = environment.Console;
                }
                environment.Console.WriteLine($"Running {test.Name}");
                await test.Run(instance);
                environment.Console.WriteLine($"Finished {test.Name}");
            }
        }
        //await Parallel.ForEachAsync(testSuite.Tests, async (test, _) => await test.Run());
        TestBase.CleanupSession();
    }
}