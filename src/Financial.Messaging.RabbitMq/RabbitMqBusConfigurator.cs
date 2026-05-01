using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.RabbitMq.Models;
using Financial.Messaging.RabbitMq.Topology;

namespace Financial.Messaging.RabbitMq;

/// <summary>
/// Configures the RabbitMQ message bus by registering receive endpoints,
/// building topology (exchanges, queues, bindings), and tracking endpoint metadata.
/// </summary>
internal sealed class RabbitMqBusConfigurator : IBusConfigurator
{
    private readonly TopologyBuilder _topologyBuilder;
    private readonly RabbitMqEntityNameFormatter _formatter;
    private readonly List<EndpointRegistration> _endpoints = [];

    public IReadOnlyList<EndpointRegistration> Endpoints => _endpoints;

    public RabbitMqBusConfigurator(
        TopologyBuilder topologyBuilder,
        RabbitMqEntityNameFormatter formatter)
    {
        _topologyBuilder = topologyBuilder;
        _formatter = formatter;
    }

    /// <summary>
    /// Registers a receive endpoint that routes <typeparamref name="TMessage"/> to <typeparamref name="THandler"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message type to consume.</typeparam>
    /// <typeparam name="THandler">The handler type to process the message.</typeparam>
    /// <returns>The current <see cref="IBusConfigurator"/> instance for fluent chaining.</returns>
    public IBusConfigurator ReceiveEndpoint<TMessage, THandler>()
        where TMessage : class, IMessage
        where THandler : class, IMessageHandler<TMessage>
    {
        var queueName = _formatter.FormatQueueName<THandler>();

        _topologyBuilder
            .AddChannel<TMessage>()
            .AddEndpoint(queueName)
            .AddRoute<TMessage>(queueName);

        _endpoints.Add(new EndpointRegistration(
            QueueName: queueName,
            MessageType: typeof(TMessage),
            HandlerType: typeof(THandler)));

        return this;
    }
}
