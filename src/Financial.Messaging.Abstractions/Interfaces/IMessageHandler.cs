namespace Financial.Messaging.Abstractions.Messages;

public interface IMessageHandler<in TMessage>
    where TMessage : class, IMessage
{
    /// <summary>Processes the given message asynchronously.</summary>
    /// <param name="message">The message to handle.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);
}
