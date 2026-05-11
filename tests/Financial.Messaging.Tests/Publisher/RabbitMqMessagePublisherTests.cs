using Financial.Messaging.RabbitMq;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.Tests.Helpers;
using NSubstitute;
using RabbitMQ.Client;

namespace Financial.Messaging.Tests.Publisher;

public sealed class RabbitMqMessagePublisherTests
{
    private static (IChannel channel, RabbitMqMessagePublisher publisher) BuildPublisher()
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);

        var connection = Substitute.For<IConnection>();
        connection.CreateChannelAsync(cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(channel));

        var connectionFactory = Substitute.For<IConnectionFactory>();
        connectionFactory.CreateConnectionAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(connection));

        var formatter = new RabbitMqEntityNameFormatter(string.Empty);
        var publisher = new RabbitMqMessagePublisher(connectionFactory, formatter);

        return (channel, publisher);
    }

    [Fact]
    public async Task PublishAsync_PublishesToCorrectExchange()
    {
        var (channel, publisher) = BuildPublisher();

        await publisher.PublishAsync(new TestMessage(), CancellationToken.None);

        await channel.Received(1).BasicPublishAsync(
            exchange: "test-message",
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: Arg.Any<BasicProperties>(),
            body: Arg.Any<ReadOnlyMemory<byte>>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_ReusesSameChannel_WhenAlreadyOpen()
    {
        var (channel, publisher) = BuildPublisher();

        await publisher.PublishAsync(new TestMessage(), CancellationToken.None);
        await publisher.PublishAsync(new TestMessage(), CancellationToken.None);

        await channel.Received(2).BasicPublishAsync(
            exchange: Arg.Any<string>(),
            routingKey: Arg.Any<string>(),
            mandatory: Arg.Any<bool>(),
            basicProperties: Arg.Any<BasicProperties>(),
            body: Arg.Any<ReadOnlyMemory<byte>>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PublishAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var (_, publisher) = BuildPublisher();

        await publisher.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            publisher.PublishAsync(new TestMessage(), CancellationToken.None));
    }
}
