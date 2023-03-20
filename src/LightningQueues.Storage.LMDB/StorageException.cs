using System;
using LightningDB;

namespace LightningQueues.Storage.LMDB;

public class StorageException : Exception
{
    public StorageException(string message, MDBResultCode resultCode) : base(message)
    {
        ResultCode = resultCode;
    }
    
    public MDBResultCode ResultCode { get; }
}