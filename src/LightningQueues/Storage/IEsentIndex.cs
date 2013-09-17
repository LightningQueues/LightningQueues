using System.Text;
using LightningQueues.Model;
using Microsoft.Isam.Esent.Interop;

namespace LightningQueues.Storage
{
    public interface IEsentIndex
    {
        Session Session { get; set; }
        Table Table { get; set; }
        bool SeekTo();
    }

    public class StringValueIndex : IEsentIndex
    {
        private readonly string _indexName;
        private readonly string _value;

        public StringValueIndex(string indexName, string value)
        {
            _indexName = indexName;
            _value = value;
        }

        public Session Session { get; set; }
        public Table Table { get; set; }

        public bool SeekTo()
        {
            Api.JetSetCurrentIndex(Session, Table, _indexName);
            Api.MakeKey(Session, Table, _value, Encoding.Unicode, MakeKeyGrbit.NewKey);

            if (Api.TrySeek(Session, Table, SeekGrbit.SeekGE) == false)
                return false;

            Api.MakeKey(Session, Table, _value, Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
            try
            {
                Api.JetSetIndexRange(Session, Table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
            }
            catch (EsentErrorException e)
            {
                if (e.Error != JET_err.NoCurrentRecord)
                    throw;
                return false;
            }
            return true;
        }
    }

    public class StartingIndex : IEsentIndex
    {
        public Session Session { get; set; }
        public Table Table { get; set; }

        public bool SeekTo()
        {
            Api.MoveBeforeFirst(Session, Table);
            return Api.TryMoveNext(Session, Table);
        }
    }

    public class MessageIdIndex : IEsentIndex
    {
        private readonly MessageId _id;

        public MessageIdIndex(MessageId id)
        {
            _id = id;
        }

        public Session Session { get; set; }
        public Table Table { get; set; }

        public bool SeekTo()
        {
            Api.JetSetCurrentIndex(Session, Table, "by_id");
            Api.MakeKey(Session, Table, _id.SourceInstanceId.ToByteArray(), MakeKeyGrbit.NewKey);
            Api.MakeKey(Session, Table, _id.MessageIdentifier, MakeKeyGrbit.None);

            if (Api.TrySeek(Session, Table, SeekGrbit.SeekEQ) == false)
                return false;

            Api.MakeKey(Session, Table, _id.SourceInstanceId.ToByteArray(), MakeKeyGrbit.NewKey);
            Api.MakeKey(Session, Table, _id.MessageIdentifier, MakeKeyGrbit.None);
            try
            {
                Api.JetSetIndexRange(Session, Table,
                    SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);
            }
            catch (EsentErrorException e)
            {
                if (e.Error != JET_err.NoCurrentRecord)
                    throw;
                return false;
            }
            return true;
        }


    }

    public class PositionalIndexFromLast : IEsentIndex
    {
        private readonly int _numberOfItemsFromEnd;

        public PositionalIndexFromLast(int numberOfItemsFromEnd)
        {
            _numberOfItemsFromEnd = numberOfItemsFromEnd;
        }

        public Session Session { get; set; }
        public Table Table { get; set; }

        public bool SeekTo()
        {
            Api.MoveAfterLast(Session, Table);
            try
            {
                Api.JetMove(Session, Table, -_numberOfItemsFromEnd, MoveGrbit.None);
            }
            catch (EsentErrorException e)
            {
                if (e.Error == JET_err.NoCurrentRecord)
                    return false;
                throw;
            }
            return true;
        }
    }
}