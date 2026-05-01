namespace Financial.Messaging.RabbitMq.Models;

/// <summary>
/// Holds the resolved names and types for a registered receive endpoint.
/// </summary>
internal sealed record EndpointRegistration(
    string QueueName,
    Type MessageType,
    Type HandlerType);