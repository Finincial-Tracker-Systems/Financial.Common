using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using RabbitMQ.Client;
using System.Text.Json;

namespace Financial.Messaging.RabbitMq;

/// <summary>
/// Publishes messages to RabbitMQ exchanges.
/// Lazily creates its own connection and channel on first use.
/// Relies on <see cref="ConnectionFactory.AutomaticRecoveryEnabled"/> for reconnection.
/// </summary>
internal sealed class RabbitMqMessagePublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly RabbitMqEntityNameFormatter _formatter;

    private IConnection? _connection;
    private IChannel? _channel;
    private bool _disposed;

    public RabbitMqMessagePublisher(
        ConnectionFactory connectionFactory,
        RabbitMqEntityNameFormatter formatter)
    {
        _connectionFactory = connectionFactory;
        _formatter = formatter;
    }

    /// <summary>
    /// Serializes <paramref name="message"/> to JSON and publishes it
    /// to the fanout exchange derived from <typeparamref name="TMessage"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message type, used to resolve the target exchange name.</typeparam>
    /// <param name="message">The message instance to publish.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : Message
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var channel = await GetOrCreateChannelAsync(cancellationToken);
        var exchangeName = _formatter.FormatExchangeName<TMessage>();
        var body = JsonSerializer.SerializeToUtf8Bytes(message);

        var properties = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };

        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns the existing channel if it is still open, otherwise lazily creates
    /// the connection (if needed) and opens a new channel.
    /// </summary>
    private async ValueTask<IChannel> GetOrCreateChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel?.IsOpen == true)
        {
            return _channel;
        }

        _connection ??= await _connectionFactory.CreateConnectionAsync(cancellationToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

        return _channel;
    }

    /// <summary>
    /// Disposes the channel and connection. Safe to call multiple times.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        _connection?.Dispose();

        GC.SuppressFinalize(this);
    }
}