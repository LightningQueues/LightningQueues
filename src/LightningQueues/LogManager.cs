using System;
using NLog;

namespace LightningQueues
{
    public static class LogManager
    {
        public static Logger GetLogger<T>()
        {
            return GetLogger(typeof(T));
        }

        public static Logger GetLogger(Type type)
        {
            return NLog.LogManager.GetLogger(type.FullName);
        }
    }
}