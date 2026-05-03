namespace Financial.Messaging.Abstractions.Messages;

public interface IBusConfigurator
{
    /// <summary>Routes <typeparamref name="TMessage"/> to <typeparamref name="THandler"/>.</summary>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The handler that processes the message.</typeparam>
    /// <returns>The configurator instance for fluent chaining.</returns>
    IBusConfigurator ReceiveEndpoint<TMessage, THandler>()
        where TMessage : Message
        where THandler : class, IMessageHandler<TMessage>;
}
