using Financial.Messaging.RabbitMq;
using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.RabbitMq.Topology;
using Financial.Messaging.Tests.Helpers;


namespace Financial.Messaging.Tests.Configurator;

public sealed class RabbitMqBusConfiguratorTests
{
    private readonly RabbitMqBusConfigurator _configurator;

    public RabbitMqBusConfiguratorTests()
    {
        var options = new RabbitMqOptions { Host = "localhost", Port = 5672, Username = "guest", Password = "guest" };
        var formatter = new RabbitMqEntityNameFormatter(string.Empty);
        var topology = new TopologyBuilder(formatter, options);
        _configurator = new RabbitMqBusConfigurator(topology, formatter);
    }

    [Fact]
    public void ReceiveEndpoint_RegistersOneEndpoint()
    {
        _configurator.ReceiveEndpoint<TestMessage, TestHandler>();

        Assert.Single(_configurator.Endpoints);
    }

    [Fact]
    public void ReceiveEndpoint_SetsCorrectQueueName()
    {
        _configurator.ReceiveEndpoint<TestMessage, TestHandler>();

        Assert.Equal("test", _configurator.Endpoints[0].QueueName);
    }

    [Fact]
    public void ReceiveEndpoint_SetsCorrectMessageType()
    {
        _configurator.ReceiveEndpoint<TestMessage, TestHandler>();

        Assert.Equal(typeof(TestMessage), _configurator.Endpoints[0].MessageType);
    }

    [Fact]
    public void ReceiveEndpoint_SetsCorrectHandlerType()
    {
        _configurator.ReceiveEndpoint<TestMessage, TestHandler>();

        Assert.Equal(typeof(TestHandler), _configurator.Endpoints[0].HandlerType);
    }

    [Fact]
    public void ReceiveEndpoint_MultipleEndpoints_AllRegistered()
    {
        _configurator
            .ReceiveEndpoint<TestMessage, TestHandler>()
            .ReceiveEndpoint<AnotherMessage, AnotherHandler>();

        Assert.Equal(2, _configurator.Endpoints.Count);
    }

    [Fact]
    public void ReceiveEndpoint_ReturnsConfigurator_ForFluentChaining()
    {
        var result = _configurator.ReceiveEndpoint<TestMessage, TestHandler>();

        Assert.Same(_configurator, result);
    }
}
