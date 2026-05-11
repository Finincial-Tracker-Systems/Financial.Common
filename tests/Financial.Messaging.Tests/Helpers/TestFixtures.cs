using Financial.Messaging.Abstractions.Messages;

namespace Financial.Messaging.Tests.Helpers;

internal sealed class TestMessage : Message
{
    public override MessageHeader Header { get; init; } = new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        OccuredOnUtc = DateTime.UtcNow,
        HeaderVersion = "1.0"
    };
}

internal sealed class AnotherMessage : Message
{
    public override MessageHeader Header { get; init; } = new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        OccuredOnUtc = DateTime.UtcNow,
        HeaderVersion = "1.0"
    };
}

internal sealed class TestHandler : IMessageHandler<TestMessage>
{
    public Task HandleAsync(TestMessage message, CancellationToken cancellationToken = default) => Task.CompletedTask;
}

internal sealed class AnotherHandler : IMessageHandler<AnotherMessage>
{
    public Task HandleAsync(AnotherMessage message, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
