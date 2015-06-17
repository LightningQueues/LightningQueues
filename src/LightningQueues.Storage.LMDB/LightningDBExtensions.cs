using LightningDB;
using System.Text;

namespace LightningQueues.Storage.LMDB
{
    public static class LightningDBExtensions
    {
        public static void Put(this LightningTransaction transaction, LightningDatabase db, string key, string value)
        {
            transaction.Put(db, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
        }
    }
}