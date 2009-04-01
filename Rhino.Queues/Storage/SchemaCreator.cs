using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.Queues.Storage
{
    public class SchemaCreator
    {
        private readonly Session session;
        public const string SchemaVersion = "1.5";

        public SchemaCreator(Session session)
        {
            this.session = session;
        }

        public void Create(string database)
        {
            JET_DBID dbid;
            Api.JetCreateDatabase(session, database, null, out dbid, CreateDatabaseGrbit.None);
            try
            {
                using (var tx = new Transaction(session))
                {
                    CreateDetailsTable(dbid);
                    CreateQueuesTable(dbid);
                    CreateTransactionTable(dbid);
                    CreateRecoveryTable(dbid);
                    CreateOutgoingTable(dbid);
                    CreateOutgoingHistoryTable(dbid);

                    tx.Commit(CommitTransactionGrbit.None);
                }
            }
            finally
            {
                Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
            }
        }

        private void CreateOutgoingHistoryTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "outgoing_history", 16, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "msg_id", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);


            Api.JetAddColumn(session, tableid, "tx_id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "address", new JET_COLUMNDEF
            {
                cbMax = 255,
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "port", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "number_of_retries", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "size_of_data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "time_to_send", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "sent_at", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "send_status", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed,
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "queue", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL,
                cp = JET_CP.Unicode
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.LongBinary,
            }, null, 0, out columnid);

            var indexDef = "+msg_id\0\0";
            Api.JetCreateIndex(session, tableid, "pk", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);

            indexDef = "+tx_id\0\0";
            Api.JetCreateIndex(session, tableid, "by_tx_id", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);
        }

        private void CreateOutgoingTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "outgoing", 16, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "msg_id", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnAutoincrement | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);


            Api.JetAddColumn(session, tableid, "tx_id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "address", new JET_COLUMNDEF
            {
                cbMax = 255,
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "port", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "number_of_retries", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "size_of_data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "time_to_send", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            },null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "sent_at", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "send_status", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed,
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "queue", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL,
                cp = JET_CP.Unicode
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "data", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.LongBinary,
            }, null, 0, out columnid);

            var indexDef = "+msg_id\0\0";
            Api.JetCreateIndex(session, tableid, "pk", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);

            indexDef = "+tx_id\0\0";
            Api.JetCreateIndex(session, tableid, "by_tx_id", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);
        }

        private void CreateRecoveryTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "recovery", 16, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "tx_id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "recovery_info", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            const string indexDef = "+tx_id\0\0";
            Api.JetCreateIndex(session, tableid, "pk", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);
        }

        private void CreateDetailsTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "details", 16, 100, out tableid);
            JET_COLUMNID id;
            Api.JetAddColumn(session, tableid, "id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out id);

            JET_COLUMNID schemaVersion;
            Api.JetAddColumn(session, tableid, "schema_version", new JET_COLUMNDEF
            {
                cbMax = Encoding.Unicode.GetByteCount(SchemaVersion),
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnFixed
            }, null, 0, out schemaVersion);


            using (var update = new Update(session, tableid, JET_prep.Insert))
            {
                Api.SetColumn(session, tableid, id, Guid.NewGuid().ToByteArray());
                Api.SetColumn(session, tableid, schemaVersion, SchemaVersion, Encoding.Unicode);
                update.Save();
            }
        }

        private void CreateTransactionTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "transactions", 16, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "tx_id", new JET_COLUMNDEF
            {
                cbMax = 16,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL|ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "local_id", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnAutoincrement
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "queue", new JET_COLUMNDEF
            {
                cbMax = 255,
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.Text,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "bookmark_size", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "bookmark_data", new JET_COLUMNDEF
            {
                cbMax = 256,
                coltyp = JET_coltyp.Binary,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            Api.JetAddColumn(session, tableid, "value_to_restore", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            var indexDef = "+tx_id\0local_id\0\0";
            Api.JetCreateIndex(session, tableid, "pk", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);

            indexDef = "+tx_id\0\0";
            Api.JetCreateIndex(session, tableid, "by_tx_id", CreateIndexGrbit.IndexDisallowNull, indexDef, indexDef.Length,
                               100);
        }
        private void CreateQueuesTable(JET_DBID dbid)
        {
            JET_TABLEID tableid;
            Api.JetCreateTable(session, dbid, "queues", 16, 100, out tableid);
            JET_COLUMNID columnid;

            Api.JetAddColumn(session, tableid, "name", new JET_COLUMNDEF
            {
                cbMax = 255,
                coltyp = JET_coltyp.Text,
                cp = JET_CP.Unicode,
                grbit = ColumndefGrbit.ColumnNotNULL
            }, null, 0, out columnid);

            var defaultValue = BitConverter.GetBytes(0);
            Api.JetAddColumn(session, tableid, "number_of_messages", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.Long,
                grbit = ColumndefGrbit.ColumnFixed | ColumndefGrbit.ColumnNotNULL | ColumndefGrbit.ColumnEscrowUpdate
            }, defaultValue, defaultValue.Length, out columnid);

            Api.JetAddColumn(session, tableid, "created_at", new JET_COLUMNDEF
            {
                coltyp = JET_coltyp.DateTime,
                grbit = ColumndefGrbit.ColumnFixed
            }, null, 0, out columnid);

            const string indexDef = "+name\0\0";
            Api.JetCreateIndex(session, tableid, "pk", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length,
                               100);
        }
    }
}