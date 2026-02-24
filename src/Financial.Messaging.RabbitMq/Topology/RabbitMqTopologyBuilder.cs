using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using RabbitMQ.Client;

namespace Financial.Messaging.RabbitMq.Topology;

internal sealed class RabbitMqTopologyBuilder
{
    private readonly List<Func<IChannel, Task>> _declarations = new();
    private readonly RabbitMqEntityNameFormatter _formatter;
    private readonly RabbitMqOptions _rabbitMqOptions;

    public RabbitMqTopologyBuilder(RabbitMqEntityNameFormatter formatter, RabbitMqOptions rabbitMqOptions)
    {
        _formatter = formatter;
        _rabbitMqOptions = rabbitMqOptions;
    }

    /// <summary>
    /// Registers a declaration for a Fanout exchange associated with the specified message type.
    /// </summary>
    /// <typeparam name="TMessage">The type of message the exchange will handle.</typeparam>
    /// <returns>The current <see cref="RabbitMqTopologyBuilder"/> instance for method chaining.</returns>
    public RabbitMqTopologyBuilder AddChannel<TMessage>() where TMessage : class, IMessage
    {
        var exchangeName = _formatter.FormatExchangeName<TMessage>();

        _declarations.Add(channel => channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: _rabbitMqOptions.Durable,
            autoDelete: false));

        return this;
    }

    /// <summary>
    /// Registers a declaration for a queue. If Dead Lettering is enabled in options,
    /// it automatically creates a corresponding DLQ and configures the main queue to route failed messages to it.
    /// </summary>
    /// <param name="queueName">The name of the primary queue to be declared.</param>
    /// <returns>The current <see cref="RabbitMqTopologyBuilder"/> instance for method chaining.</returns>
    public RabbitMqTopologyBuilder AddEndpoint(string queueName)
    {
        _declarations.Add(async channel =>
        {
            var args = new Dictionary<string, object?>();

            if (_rabbitMqOptions.EnableDeadLetter)
            {
                var dlqName = _formatter.FormatDeadLetterQueueName(queueName);

                await channel.QueueDeclareAsync(
                    queue: dlqName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false);

                args["x-dead-letter-exchange"] = string.Empty;
                args["x-dead-letter-routing-key"] = dlqName;
            }

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: args);
        });

        return this;
    }

    /// <summary>
    /// Registers a declaration to bind an exchange (derived from <typeparamref name="TMessage"/>) to a specified queue.
    /// </summary>
    /// <typeparam name="TMessage">The message type whose exchange should be bound.</typeparam>
    /// <param name="queueName">The name of the queue to bind to the exchange.</param>
    public RabbitMqTopologyBuilder AddRoute<TMessage>(string queueName)
        where TMessage : class, IMessage
    {
        var exchangeName = _formatter.FormatExchangeName<TMessage>();

        _declarations.Add(channel => channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: string.Empty));

        return this;
    }

    /// <summary>
    /// Executes all registered topology declarations sequentially against the provided RabbitMQ channel.
    /// </summary>
    /// <param name="channel">The RabbitMQ channel used to execute the declarations.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation of applying the topology.</returns>
    public async Task ApplyAsync(IChannel channel, CancellationToken cancellationToken)
    {
        foreach (var _declaration in _declarations)
        {
            await _declaration(channel);
        }
    }
}
