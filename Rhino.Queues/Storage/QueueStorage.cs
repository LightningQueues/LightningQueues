using System;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Common.Logging;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.Queues.Storage
{
	public class QueueStorage : CriticalFinalizerObject, IDisposable
	{
		private readonly ILog log = LogManager.GetLogger(typeof(QueueStorage));
		private JET_INSTANCE instance;
	    private readonly string database;
	    private readonly string path;
	    private ColumnsInformation columnsInformation;
	    private readonly QueueManagerConfiguration configuration;

	    private readonly ReaderWriterLockSlim usageLock = new ReaderWriterLockSlim();

		public Guid Id { get; private set; }

		public QueueStorage(string database, QueueManagerConfiguration configuration)
		{
		    this.configuration = configuration;
		    this.database = database;
		    path = database;
			if (Path.IsPathRooted(database) == false)
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
			this.database = Path.Combine(path, Path.GetFileName(database));
			Api.JetCreateInstance(out instance, database + Guid.NewGuid());
		}

		public void Initialize()
		{
			ConfigureInstance(instance);
			try
			{
				Api.JetInit(ref instance);

				EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				LoadColumnInformation();
			}
			catch (Exception e)
			{
				Dispose();
				throw new InvalidOperationException("Could not open queue: " + database, e);
			}
		}

		private void LoadColumnInformation()
		{
			columnsInformation = new ColumnsInformation();
			instance.WithDatabase(database, (session, dbid) =>
			{
				using (var table = new Table(session, dbid, "subqueues", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.SubqueuesColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "outgoing_history", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.OutgoingHistoryColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "outgoing", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.OutgoingColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "recovery", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.RecoveryColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "transactions", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.TxsColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "queues", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.QueuesColumns = Api.GetColumnDictionary(session, table);
				}
				using (var table = new Table(session, dbid, "recveived_msgs", OpenTableGrbit.ReadOnly))
				{
					columnsInformation.RecveivedMsgsColumns = Api.GetColumnDictionary(session, table);
				}
			});
		}

		private void ConfigureInstance(JET_INSTANCE jetInstance)
		{
			new InstanceParameters(jetInstance)
			{
				CircularLog = true,
				Recovery = true,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(path, "temp"),
				SystemDirectory = Path.Combine(path, "system"),
				LogFileDirectory = Path.Combine(path, "logs"),
				MaxVerPages = 8192,
				MaxTemporaryTables = 8192
			};
		}

		private void SetIdFromDb()
		{
			try
			{
				instance.WithDatabase(database, (session, dbid) =>
				{
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
					{
						Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
						var columnids = Api.GetColumnDictionary(session, details);
						var column = Api.RetrieveColumn(session, details, columnids["id"]);
						Id = new Guid(column);
						var schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
						if (schemaVersion != SchemaCreator.SchemaVersion)
							throw new InvalidOperationException("The version on disk (" + schemaVersion + ") is different that the version supported by this library: " + SchemaCreator.SchemaVersion + Environment.NewLine +
																"You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.");
					}
				});
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." + Environment.NewLine +
													"You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", e);
			}
		}

		private void EnsureDatabaseIsCreatedAndAttachToDatabase()
		{
			using (var session = new Session(instance))
			{
				try
				{
					Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
					return;
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.DatabaseDirtyShutdown)
					{
						try
						{
							using (var recoverInstance = new Instance("Recovery instance for: " + database))
							{
								recoverInstance.Init();
								using (var recoverSession = new Session(recoverInstance))
								{
									ConfigureInstance(recoverInstance.JetInstance);
									Api.JetAttachDatabase(recoverSession, database,
														  AttachDatabaseGrbit.DeleteCorruptIndexes);
									Api.JetDetachDatabase(recoverSession, database);
								}
							}
						}
						catch (Exception)
						{
						}

						Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						return;
					}
					if (e.Error != JET_err.FileNotFound)
						throw;
				}

				new SchemaCreator(session).Create(database);
				Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
			}
		}

		public void Dispose()
		{
			usageLock.EnterWriteLock();
			try
			{
				log.Debug("Disposing queue storage");
				try
				{
					Api.JetTerm2(instance, TermGrbit.Complete);
					GC.SuppressFinalize(this);
				}
				catch (Exception e)
				{
					log.Error("Could not dispose of queue storage properly", e);
					throw;
				}
			}
			finally
			{
				usageLock.ExitWriteLock();
			}
		}

		public void DisposeRudely()
		{
			usageLock.EnterWriteLock();
			try
			{
				log.Debug("Rudely disposing queue storage");
				try
				{
					Api.JetTerm2(instance, TermGrbit.Abrupt);
					GC.SuppressFinalize(this);
				}
				catch (Exception e)
				{
					log.Error("Could not dispose of queue storage properly", e);
					throw;
				}
			}
			finally
			{
				usageLock.ExitWriteLock();
			}
		}


		~QueueStorage()
		{
			try
			{
				log.Warn("Disposing esent resources from finalizer! You should call QueueStorage.Dispose() instead!");
				Api.JetTerm2(instance, TermGrbit.Complete);
			}
			catch (Exception exception)
			{
				try
				{
					log.Error("Failed to dispose esent instance from finalizer, trying abrupt termination.", exception);
					try
					{
						Api.JetTerm2(instance, TermGrbit.Abrupt);
					}
					catch (Exception e)
					{
						log.Error("Could not dispose esent instance abruptly", e);
					}
				}
				catch
				{
				}
			}
		}

		public void Global(Action<GlobalActions> action)
		{
			var shouldTakeLock = usageLock.IsReadLockHeld == false;
			try
			{
				if (shouldTakeLock)
					usageLock.EnterReadLock();
				using (var qa = new GlobalActions(instance, columnsInformation, database, Id, configuration))
				{
					action(qa);
				}
			}
			finally 
			{
				if(shouldTakeLock)
					usageLock.ExitReadLock();
			}
		}

		public void Send(Action<SenderActions> action)
		{
			var shouldTakeLock = usageLock.IsReadLockHeld == false;
			try
			{
				if (shouldTakeLock)
					usageLock.EnterReadLock();
				using (var qa = new SenderActions(instance, columnsInformation, database, Id, configuration))
				{
					action(qa);
				}
			}
			finally
			{
				if (shouldTakeLock)
					usageLock.ExitReadLock();
			}
		}
	}
}