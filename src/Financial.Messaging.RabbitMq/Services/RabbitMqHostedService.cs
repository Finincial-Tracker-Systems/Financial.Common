using Financial.Messaging.RabbitMq.Models;
using Financial.Messaging.RabbitMq.Topology;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Financial.Messaging.RabbitMq.Services;

internal sealed class RabbitMqHostedService : IHostedService, IAsyncDisposable
{
    private readonly RabbitMqBusConfigurator _rabbitMqBusConfigurator;
    private readonly RabbitMqTopologyBuilder _rabbitMqTopologyBuilder;
    private readonly RabbitMqOptions _rabbitMqOptions;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<RabbitMqHostedService> _logger;

    private CancellationTokenSource? _stoppingCts;
    private IConnection? _connection;
    private IChannel? _publishChannel;
    private readonly List<RabbitMqMessageConsumer> _consumers = [];

    public RabbitMqHostedService(
        RabbitMqBusConfigurator rabbitMqBusConfigurator,
        RabbitMqTopologyBuilder rabbitMqTopologyBuilder,
        IOptions<RabbitMqOptions> rabbitMqOptions,
        IServiceScopeFactory serviceScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _rabbitMqBusConfigurator = rabbitMqBusConfigurator;
        _rabbitMqTopologyBuilder = rabbitMqTopologyBuilder;
        _rabbitMqOptions = rabbitMqOptions.Value;
        _serviceScopeFactory = serviceScopeFactory;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<RabbitMqHostedService>();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _rabbitMqOptions.Host,
            Port = _rabbitMqOptions.Port,
            VirtualHost = _rabbitMqOptions.VirtualHost,
            UserName = _rabbitMqOptions.Username,
            Password = _rabbitMqOptions.Password,
            AutomaticRecoveryEnabled = true
        };

        _connection = await factory.CreateConnectionAsync(cancellationToken);

        _publishChannel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await _rabbitMqTopologyBuilder.ApplyAsync(_publishChannel, cancellationToken);

        // Create linked canclellation token source to signal shutdown to consumers when StopAsync is called
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        foreach (var endpoint in _rabbitMqBusConfigurator.Endpoints)
        {
            await StartConsumerAsync(endpoint, _stoppingCts.Token);
        }

        _logger.LogInformation(
            "{MethodName}. RabbitMQ bus started. {Count} consumer(s) listening",
            nameof(StartAsync),
           _rabbitMqBusConfigurator.Endpoints.Count);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping RabbitMQ bus");

        // Signal consumers to stop accepting new messages
        if (_stoppingCts is not null)
        {
            await _stoppingCts.CancelAsync();
        }

        foreach (var consumer in _consumers)
        {
            await consumer.StopAsync(cancellationToken);
        }

        if (_publishChannel is not null)
        {
            await _publishChannel.CloseAsync(cancellationToken);
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stoppingCts?.Dispose();

        if (_publishChannel is not null && _publishChannel.IsOpen)
        {
            await _publishChannel.DisposeAsync();
        }

        if (_connection is not null && _connection.IsOpen)
        {
            await _connection.DisposeAsync();
        }

        GC.SuppressFinalize(this);
    }

    private async Task StartConsumerAsync(EndpointRegistration endpoint, CancellationToken cancellationToken)
    {
        var channel = await _connection!.CreateChannelAsync(cancellationToken: cancellationToken);
        var logger = _loggerFactory.CreateLogger<RabbitMqMessageConsumer>();

        var consumer = new RabbitMqMessageConsumer(
            channel,
            endpoint,
            _serviceScopeFactory,
            logger);

        _consumers.Add(consumer);
        await consumer.StartAsync(cancellationToken);
    }
}
