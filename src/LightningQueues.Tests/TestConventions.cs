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
        //await RunTestsSynchronous(testSuite);
        await RunTestsInParallel(testSuite);
        TestBase.CleanupSession();
    }

    private async Task RunTestsInParallel(TestSuite testSuite)
    {
        await Parallel.ForEachAsync(testSuite.TestClasses, async (testClass, _) =>
        {
            await Parallel.ForEachAsync(testClass.Tests, async (test, _) =>
            {
                await ActualRunTest(testClass, test);
            });
        });
    }

    private async Task ActualRunTest(TestClass testClass, Test test)
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

    private async Task RunTestsSynchronous(TestSuite testSuite)
    {
        foreach (var testClass in testSuite.TestClasses)
        {
            foreach (var test in testClass.Tests)
            {
                await ActualRunTest(testClass, test);
            }
        }
    }
}