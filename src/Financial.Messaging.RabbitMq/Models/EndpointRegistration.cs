namespace Financial.Messaging.RabbitMq.Models;

internal sealed record EndpointRegistration(
    string QueueName,
    Type MessageType,
    Type HandlerType);