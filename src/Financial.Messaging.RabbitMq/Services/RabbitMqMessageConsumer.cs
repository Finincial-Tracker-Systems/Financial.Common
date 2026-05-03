using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace Financial.Messaging.RabbitMq.Services;

/// <summary>
/// Listens on a single RabbitMQ queue and dispatches received messages
/// through the pre-compiled invoke delegate on the endpoint registration.
/// </summary>
internal sealed class RabbitMqMessageConsumer : IAsyncDisposable
{
    private readonly IChannel _channel;
    private readonly EndpointRegistration _registration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqMessageConsumer> _logger;
    private readonly CancellationTokenSource _stoppingCts = new();

    public RabbitMqMessageConsumer(
        IChannel channel,
        EndpointRegistration registration,
        IServiceScopeFactory scopeFactory,
        RabbitMqOptions options,
        ILogger<RabbitMqMessageConsumer> logger)
    {
        _channel = channel;
        _registration = registration;
        _scopeFactory = scopeFactory;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Starts consuming messages from the registered queue.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: _options.PrefetchCount,
            global: false,
            cancellationToken: cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageReceivedAsync;

        await _channel.BasicConsumeAsync(
            queue: _registration.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);

        _logger.LogInformation(
            "{MethodName}. Started consuming queue '{Queue}' with handler '{Handler}'.",
            _registration.QueueName,
            _registration.HandlerType.Name,
            nameof(StartAsync));
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        var deliveryTag = args.DeliveryTag;

        try
        {
            var message = JsonSerializer.Deserialize(args.Body.Span, _registration.MessageType);

            await using var scope = _scopeFactory.CreateAsyncScope();

            var handlerType = typeof(IMessageHandler<>).MakeGenericType(_registration.MessageType);
            var handler = scope.ServiceProvider.GetRequiredService(handlerType);

            await (Task)handlerType
                .GetMethod(nameof(IMessageHandler<Message>.HandleAsync))!
                .Invoke(handler, [message, _stoppingCts.Token])!;

            await _channel.BasicAckAsync(deliveryTag, multiple: false);
        }
        catch (OperationCanceledException) when (_stoppingCts.IsCancellationRequested)
        {
            _logger.LogWarning(
                "{MethodName}. Message processing cancelled during shutdown on queue '{Queue}'. Requeuing.",
                nameof(OnMessageReceivedAsync),
                _registration.QueueName);

            await NackSafeAsync(deliveryTag, requeue: true);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "{MethodName}. Error processing message from queue '{Queue}'. Delivery tag: {DeliveryTag}.",
                nameof(OnMessageReceivedAsync),
                _registration.QueueName,
                deliveryTag);

            await NackSafeAsync(deliveryTag, requeue: false);
        }
    }

    private async Task NackSafeAsync(ulong deliveryTag, bool requeue)
    {
        try
        {
            await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "{MethodName}. Failed to nack delivery tag {DeliveryTag}.",
                nameof(NackSafeAsync),
                deliveryTag);
        }
    }

    /// <summary>
    /// Signals the consumer to stop processing, closes the channel gracefully, and releases all held resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _stoppingCts.CancelAsync();

        try
        {
            await _channel.CloseAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing consumer channel for queue '{Queue}'.", _registration.QueueName);
        }

        await _channel.DisposeAsync();
        _stoppingCts.Dispose();

        GC.SuppressFinalize(this);
    }
}