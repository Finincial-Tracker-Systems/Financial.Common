using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.RabbitMq.Models;
using Financial.Messaging.RabbitMq.Topology;

namespace Financial.Messaging.RabbitMq;

internal sealed class RabbitMqBusConfigurator : IBusConfigurator
{
    private readonly RabbitMqTopologyBuilder _topologyBuilder;
    private readonly RabbitMqEntityNameFormatter _formatter;
    private readonly List<EndpointRegistration> _endpoints = new();

    public IReadOnlyList<EndpointRegistration> Endpoints => _endpoints;

    public RabbitMqBusConfigurator(
        RabbitMqTopologyBuilder topologyBuilder,
        RabbitMqEntityNameFormatter formatter)
    {
        _topologyBuilder = topologyBuilder;
        _formatter = formatter;
    }

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
