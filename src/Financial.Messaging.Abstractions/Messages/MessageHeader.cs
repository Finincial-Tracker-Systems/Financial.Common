namespace Financial.Messaging.Abstractions.Messages;

public class MessageHeader
{
    public Guid MessageId { get; init; }

    public Guid CorrelationId { get; init; }

    public DateTime OccuredOnUtc { get; init; }

    public string HeaderVersion { get; init; } = null!;
}
