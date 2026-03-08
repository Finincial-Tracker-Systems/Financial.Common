using System.Text.Json;
using Financial.Messaging.Abstractions;
using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Financial.Messaging.RabbitMq.Services;

internal sealed class RabbitMqMessagePublisher : IMessagePublisher
{
    private readonly IChannel _channel;
    private readonly RabbitMqEntityNameFormatter _formatter;
    private readonly ILogger<RabbitMqMessagePublisher> _logger;

    public RabbitMqMessagePublisher(
        IChannel channel,
        RabbitMqEntityNameFormatter formatter,
        ILogger<RabbitMqMessagePublisher> logger)
    {
        _channel = channel;
        _formatter = formatter;
        _logger = logger;
    }

    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : class, IMessage
    {
        var exchange = _formatter.FormatExchangeName<TMessage>();
        var body = JsonSerializer.SerializeToUtf8Bytes(message);

        var properties = new BasicProperties
        {
            MessageId = message.MessageId.ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
        };

        await _channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);

        _logger.LogInformation(
            "{MethodName}. Published {MessageType} with id {MessageId} to exchange {Exchange}",
            nameof(PublishAsync),
            typeof(TMessage).Name,
            message.MessageId.ToString(),
            exchange);
    }
}