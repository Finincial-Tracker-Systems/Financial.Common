using Financial.Messaging.Abstractions.Messages;

namespace Financial.Messaging.RabbitMq.Services;

internal sealed class RabbitMqMessagePublisher : IMessagePublisher
{
    Task IMessagePublisher.PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
