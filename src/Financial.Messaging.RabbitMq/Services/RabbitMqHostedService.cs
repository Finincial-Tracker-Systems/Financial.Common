using Financial.Messaging.RabbitMq.Services;
using Financial.Messaging.RabbitMq.Topology;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Financial.Messaging.RabbitMq;

/// <summary>
/// Hosted service that owns the RabbitMQ consumer connection lifecycle
/// </summary>
internal sealed class RabbitMqHostedService : IHostedService, IAsyncDisposable
{
    private readonly ConnectionFactory _connectionFactory;
    private readonly TopologyBuilder _topologyBuilder;
    private readonly RabbitMqBusConfigurator _configurator;
    private readonly RabbitMqOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RabbitMqHostedService> _logger;

    private IConnection? _connection;
    private readonly List<RabbitMqMessageConsumer> _consumers = [];

    public RabbitMqHostedService(
        ConnectionFactory connectionFactory,
        TopologyBuilder topologyBuilder,
        RabbitMqBusConfigurator configurator,
        RabbitMqOptions options,
        IServiceProvider serviceProvider,
        ILogger<RabbitMqHostedService> logger)
    {
        _connectionFactory = connectionFactory;
        _topologyBuilder = topologyBuilder;
        _configurator = configurator;
        _options = options;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    /// <summary>
    /// Creates the connection, applies topology, and starts all registered consumers.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _connection = await _connectionFactory.CreateConnectionAsync(cancellationToken);

        _logger.LogInformation("Applying RabbitMQ topology...");
        await using var topologyChannel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
        await _topologyBuilder.ApplyAsync(topologyChannel, cancellationToken);

        _logger.LogInformation("Starting {Count} consumer(s)...", _configurator.Endpoints.Count);
        foreach (var endpoint in _configurator.Endpoints)
        {
            var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);
            var serviceScopeFactory = _serviceProvider.GetRequiredService<IServiceScopeFactory>();
            var logger = _serviceProvider.GetRequiredService<ILogger<RabbitMqMessageConsumer>>();

            var consumer = new RabbitMqMessageConsumer(
                channel,
                endpoint,
                serviceScopeFactory,
                _options,
                logger);

            await consumer.StartAsync(cancellationToken);
            _consumers.Add(consumer);
        }

        _logger.LogInformation("RabbitMQ bus started.");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping RabbitMQ bus...");

        foreach (var consumer in Enumerable.Reverse(_consumers))
        {
            await consumer.DisposeAsync();
        }

        _consumers.Clear();

        if (_connection is not null)
        {
            await _connection.CloseAsync(cancellationToken);
            _connection.Dispose();
            _connection = null;
        }

        _logger.LogInformation("RabbitMQ bus stopped.");
    }

    /// <summary>
    /// Disposes all active consumers and releases the shared connection.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }

        if (_connection is not null)
        {
            _connection.Dispose();
            _connection = null;
        }

        GC.SuppressFinalize(this);
    }
}