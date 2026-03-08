using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace Financial.Messaging.RabbitMq.Services;

internal class RabbitMqMessageConsumer : IAsyncDisposable
{
    private readonly IChannel _channel;
    private readonly EndpointRegistration _endpointRegistration;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<RabbitMqMessageConsumer> _logger;

    private string? _consumerTag;
    private int _processingCount;

    public RabbitMqMessageConsumer(
        IChannel channel,
        EndpointRegistration endpointRegistration,
        IServiceScopeFactory serviceScopeFactory,
        ILogger<RabbitMqMessageConsumer> logger)
    {
        _channel = channel;
        _endpointRegistration = endpointRegistration;
        _serviceScopeFactory = serviceScopeFactory;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 10,
            global: false,
            cancellationToken: cancellationToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += (_, ea) => OnMessageReceivedAsync(ea, cancellationToken);

        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _endpointRegistration.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);

        _logger.LogInformation(
            "{MethodName}. Consumer started on queue {Queue} for {MessageType}",
            nameof(StartAsync),
            _endpointRegistration.QueueName,
            _endpointRegistration.MessageType.Name);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumerTag is not null)
        {
            await _channel.BasicCancelAsync(_consumerTag, cancellationToken: cancellationToken);
        }

        var timeout = DateTime.UtcNow.AddSeconds(30);
        while (_processingCount > 0 && DateTime.UtcNow < timeout)
            await Task.Delay(50, cancellationToken);

        if (_processingCount > 0)
        {
            _logger.LogWarning(
                "{MethodName}. Consumer stopped with {Count} message(s) still in-flight on queue {Queue}",
                nameof(StopAsync),
                _processingCount,
                _endpointRegistration.QueueName);
        }

        if (_channel.IsOpen)
        {
            await _channel.CloseAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel.IsOpen)
        {
            await _channel.DisposeAsync();
        }

        GC.SuppressFinalize(this);
    }

    private async Task OnMessageReceivedAsync(BasicDeliverEventArgs basicDeliverEventArgs, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _processingCount);

        try
        {
            var message = (IMessage)JsonSerializer.Deserialize(basicDeliverEventArgs.Body.Span, _endpointRegistration.MessageType)!;

            await using var scope = _serviceScopeFactory.CreateAsyncScope();

            var handlerType = typeof(IMessageHandler<>).MakeGenericType(_endpointRegistration.MessageType);
            var handler = scope.ServiceProvider.GetRequiredService(handlerType);

            await (Task)handlerType
                .GetMethod(nameof(IMessageHandler<IMessage>.HandleAsync))!
                .Invoke(handler, [message, cancellationToken])!;

            await _channel.BasicAckAsync(
                basicDeliverEventArgs.DeliveryTag,
                multiple: false,
                cancellationToken);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "{MethodName}. Failed to handle {MessageType} from queue {Queue}. Details: {Message}",
                nameof(OnMessageReceivedAsync),
                _endpointRegistration.MessageType.Name,
                _endpointRegistration.QueueName,
                exception.Message);

            await _channel.BasicNackAsync(basicDeliverEventArgs.DeliveryTag, multiple: false, requeue: false, cancellationToken);
        }
        finally
        {
            Interlocked.Decrement(ref _processingCount);
        }
    }
}
