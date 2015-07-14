using System;
using System.IO;
using Microsoft.Framework.Runtime;
using Microsoft.Framework.Runtime.Infrastructure;
using Xunit;

namespace LightningQueues.Tests
{
    public class SharedTestDirectory : IDisposable
    {
        private readonly string _testTempDir;

        public SharedTestDirectory()
        {
            var locator = CallContextServiceLocator.Locator;
            var services = locator.ServiceProvider;
            var libraryManager = (ILibraryManager)services.GetService(typeof(ILibraryManager));
            var testProjectDir = Path.GetDirectoryName(libraryManager.GetLibraryInformation("LightningQueues.Tests").Path);
            _testTempDir = Path.Combine(Directory.GetParent(testProjectDir).Parent.FullName, "TestData");
        }

        public void Dispose()
        {
            Directory.Delete(_testTempDir, true);
        }

        public string CreateNewDirectoryForTest()
        {
            var path = Path.Combine(_testTempDir, Guid.NewGuid().ToString());
            Directory.CreateDirectory(path);
            return path;
        }
    }

    [CollectionDefinition("SharedTestDirectory")]
    public class SharedTestDirectoryCollection : ICollectionFixture<SharedTestDirectory>
    {
    }
}