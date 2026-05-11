using Financial.Messaging.RabbitMq.Conventions;
using Financial.Messaging.Tests.Helpers;

namespace Financial.Messaging.Tests.Conventions;

public sealed class RabbitMqEntityNameFormatterTests
{
    [Fact]
    public void FormatExchangeName_WithoutPrefix_ReturnsKebabCase()
    {
        var formatter = new RabbitMqEntityNameFormatter(string.Empty);

        var result = formatter.FormatExchangeName<TestMessage>();

        Assert.Equal("test-message", result);
    }

    [Fact]
    public void FormatExchangeName_WithPrefix_ReturnsPrefixedKebabCase()
    {
        var formatter = new RabbitMqEntityNameFormatter("financial-tracker");

        var result = formatter.FormatExchangeName<TestMessage>();

        Assert.Equal("financial-tracker.test-message", result);
    }

    [Fact]
    public void FormatQueueName_WithoutPrefix_RemovesHandlerSuffixAndConvertsToKebabCase()
    {
        var formatter = new RabbitMqEntityNameFormatter(string.Empty);

        var result = formatter.FormatQueueName<TestHandler>();

        Assert.Equal("test", result);
    }

    [Fact]
    public void FormatQueueName_WithPrefix_ReturnsPrefixedKebabCase()
    {
        var formatter = new RabbitMqEntityNameFormatter("financial-tracker");

        var result = formatter.FormatQueueName<TestHandler>();

        Assert.Equal("financial-tracker.test", result);
    }

    [Fact]
    public void FormatDeadLetterQueueName_AppendsErrorSuffix()
    {
        var formatter = new RabbitMqEntityNameFormatter(string.Empty);

        var result = formatter.FormatDeadLetterQueueName("test");

        Assert.Equal("test.error", result);
    }

    [Fact]
    public void FormatDeadLetterQueueName_WithPrefix_PreservesFullQueueName()
    {
        var formatter = new RabbitMqEntityNameFormatter(string.Empty);

        var result = formatter.FormatDeadLetterQueueName("financial-tracker.test");

        Assert.Equal("financial-tracker.test.error", result);
    }
}
