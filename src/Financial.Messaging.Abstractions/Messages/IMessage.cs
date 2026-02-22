namespace Financial.Messaging.Abstractions.Messages;

public interface IMessage
{
    /// <summary>
    /// Unique identifier of this message instance.
    /// </summary>
    Guid MessageId { init; }

    /// <summary>
    /// UTC timestamp when this message was created.
    /// </summary>
    DateTime OccuredOnUtc { init; }
}
