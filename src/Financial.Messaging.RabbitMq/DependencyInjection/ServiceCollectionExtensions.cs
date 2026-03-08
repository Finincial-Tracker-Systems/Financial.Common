using Financial.Messaging.Abstractions.Messages;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.RabbitMq.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace Financial.Messaging.RabbitMq.DependencyInjection;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the RabbitMQ message bus: connection, topology, publisher, and consumers.
    /// </summary>
    public static IServiceCollection AddRabbitMqBus(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<IBusConfigurator> configure)
    {
        services
            .AddOptions<RabbitMqOptions>()
            .Bind(configuration.GetSection(RabbitMqOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        var options = configuration
            .GetSection(RabbitMqOptions.SectionName)
            .Get<RabbitMqOptions>()
            ?? throw new InvalidOperationException(
                $"Missing configuration section '{RabbitMqOptions.SectionName}'.");

        var formatter = new RabbitMqEntityNameFormatter(options.EntityPrefix);
        var topologyBuilder = new TopologyBuilder(formatter, options);
        var configurator = new RabbitMqBusConfigurator(topologyBuilder, formatter);

        configure(configurator);

        foreach (var endpoint in configurator.Endpoints)
        {
            var handlerInterface = typeof(IMessageHandler<>).MakeGenericType(endpoint.MessageType);
            services.AddScoped(handlerInterface, endpoint.HandlerType);
        }

        // Infrastructure singletons
        services.AddSingleton(formatter);
        services.AddSingleton(options);
        services.AddSingleton(topologyBuilder);
        services.AddSingleton(configurator);

        services.AddSingleton(_ => new ConnectionFactory
        {
            HostName = options.Host,
            Port = options.Port,
            VirtualHost = options.VirtualHost,
            UserName = options.Username,
            Password = options.Password,
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
        });

        services.AddHostedService<RabbitMqHostedService>();
        services.AddSingleton<RabbitMqMessagePublisher>();

        return services;
    }
}