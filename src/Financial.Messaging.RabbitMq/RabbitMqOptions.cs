namespace Financial.Messaging.RabbitMq;

internal sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public bool Durable { get; set; } = true;

    public bool EnableDeadLetter { get; set; } = true;
}
