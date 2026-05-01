namespace Financial.Messaging.Abstractions.Messages;

public interface IMessagePublisher
{
    /// <summary>Publishes a message to the underlying transport. Routing is determined by the message type convention.</summary>
    /// <typeparam name="TMessage">The type of message to publish. Must be a reference type implementing <see cref="IMessage"/>.</typeparam>
    /// <param name="message">The message instance to publish.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
       where TMessage : class, IMessage;
}
