using Financial.Messaging.RabbitMq;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.RabbitMq.Topology;
using Financial.Messaging.Tests.Helpers;
using NSubstitute;
using RabbitMQ.Client;

namespace Financial.Messaging.Tests.Topology;

public sealed class TopologyBuilderTests
{
    private static readonly RabbitMqOptions Options = new()
    {
        Host = "localhost",
        Port = 5672,
        Username = "guest",
        Password = "guest",
        Durable = true,
        EnableDeadLetter = true
    };

    private readonly RabbitMqEntityNameFormatter _formatter = new(string.Empty);

    [Fact]
    public async Task ApplyAsync_AddChannel_DeclaresExchangeWithCorrectName()
    {
        var channel = Substitute.For<IChannel>();
        var builder = new TopologyBuilder(_formatter, Options);
        builder.AddChannel<TestMessage>();

        await builder.ApplyAsync(channel, CancellationToken.None);

        await channel.Received(1).ExchangeDeclareAsync(
            exchange: "test-message",
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ApplyAsync_AddEndpointWithDeadLetter_DeclaresDlq()
    {
        var channel = Substitute.For<IChannel>();
        var builder = new TopologyBuilder(_formatter, Options);
        builder.AddEndpoint("test");

        await builder.ApplyAsync(channel, CancellationToken.None);

        await channel.Received(1).QueueDeclareAsync(
            queue: "test.error",
            durable: Arg.Any<bool>(),
            exclusive: Arg.Any<bool>(),
            autoDelete: Arg.Any<bool>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ApplyAsync_AddEndpoint_DeclaresMainQueue()
    {
        var channel = Substitute.For<IChannel>();
        var builder = new TopologyBuilder(_formatter, Options);
        builder.AddEndpoint("test");

        await builder.ApplyAsync(channel, CancellationToken.None);

        await channel.Received(1).QueueDeclareAsync(
            queue: "test",
            durable: Arg.Any<bool>(),
            exclusive: Arg.Any<bool>(),
            autoDelete: Arg.Any<bool>(),
            arguments: Arg.Any<IDictionary<string, object?>>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ApplyAsync_AddRoute_BindsQueueToExchange()
    {
        var channel = Substitute.For<IChannel>();
        var builder = new TopologyBuilder(_formatter, Options);
        builder.AddRoute<TestMessage>("test");

        await builder.ApplyAsync(channel, CancellationToken.None);

        await channel.Received(1).QueueBindAsync(
            queue: "test",
            exchange: "test-message",
            routingKey: Arg.Any<string>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ApplyAsync_NoDeclarations_DoesNotCallChannel()
    {
        var channel = Substitute.For<IChannel>();
        var builder = new TopologyBuilder(_formatter, Options);

        await builder.ApplyAsync(channel, CancellationToken.None);

        await channel.DidNotReceive().ExchangeDeclareAsync(
            exchange: Arg.Any<string>(),
            type: Arg.Any<string>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }
}
