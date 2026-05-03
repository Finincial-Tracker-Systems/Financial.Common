namespace Financial.Messaging.Abstractions.Messages;

public abstract class Message
{
    public abstract MessageHeader Header { get; init; }
}
