namespace Rhino.Queues.Storage
{
    public enum MessageStatus
    {
        None = 0,
        InTransit = 1,
        Discarded = 2,
        Processing = 3,
        ReadyToDeliver = 4,
    }
}