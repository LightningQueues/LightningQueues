using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fixie;
using LightningDB;
using LightningQueues.Serialization;
using LightningQueues.Storage.LMDB;

namespace LightningQueues.Tests;

public class TestBase
{
   private static readonly string _tempPath = Path.Combine(Path.GetTempPath(), $"lightningqueuestests-{Environment.Version.ToString()}");
   internal TextWriter Console { get; set; }

   protected async Task QueueScenario(Action<QueueConfiguration> queueBuilder,
      Func<Queue, CancellationToken, Task> scenario, TimeSpan timeout, string queueName = "test")
   {
      using var cancellation = new CancellationTokenSource(timeout);
      var queueConfiguration = new QueueConfiguration()
         .WithDefaultsForTest(Console);
      queueBuilder(queueConfiguration);
      using var queue = queueConfiguration.BuildAndStartQueue(queueName);
      await scenario(queue, cancellation.Token);
      await cancellation.CancelAsync();
   }
   
   protected Task QueueScenario(Action<QueueConfiguration> queueBuilder,
      Func<Queue, CancellationToken, Task> scenario, string queueName = "test")
   {
      return QueueScenario(queueBuilder, scenario, TimeSpan.FromSeconds(1), queueName);
   }

   protected Task QueueScenario(Func<Queue, CancellationToken, Task> scenario, TimeSpan timeout,
      string queueName = "test")
   {
      return QueueScenario(config => { }, scenario, timeout, queueName);
   }

   protected Task QueueScenario(Func<Queue, CancellationToken, Task> scenario, string queueName = "test")
   {
      return QueueScenario(scenario, TimeSpan.FromSeconds(1), queueName);
   }
   
   protected void StorageScenario(Action<LmdbMessageStore> action)
   {
      using var store = new LmdbMessageStore(LightningEnvironment(), new MessageSerializer());
      store.CreateQueue("test");
      action(store);
   }

   protected LightningEnvironment LightningEnvironment(string path = null)
   {
      return new LightningEnvironment(path ?? TempPath(), new EnvironmentConfiguration { MaxDatabases = 5, MapSize = 1024 * 1024 * 100 });
   }

   protected static Message NewMessage(string queueName = "test", string payload = "hello")
   {
      var message = new Message
      {
         Data = Encoding.UTF8.GetBytes(payload),
         Id = MessageId.GenerateRandom(),
         Queue = queueName,
      };
      return message;
   }

   public static string TempPath()
   {
      var path = Path.Combine(_tempPath, Guid.NewGuid().ToString());
      Directory.CreateDirectory(path);
      return path;
   }
   
   protected LightningEnvironment CreateEnvironment() => 
      new(TempPath(), new EnvironmentConfiguration { MaxDatabases = 5, MapSize = 1024 * 1024 * 100 });
   
   public static void CleanupSession()
   {
      if(Directory.Exists(_tempPath))
         Directory.Delete(_tempPath, true);
   }
}