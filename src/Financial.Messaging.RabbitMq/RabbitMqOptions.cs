namespace Financial.Messaging.RabbitMq;

/// <summary>RabbitMQ connection and behaviour settings.</summary>
internal sealed class RabbitMqOptions
{
    /// <summary>Configuration section key.</summary>
    public const string SectionName = "RabbitMq";

    /// <summary>Broker hostname.</summary>
    public required string Host { get; set; }

    /// <summary>Broker port.</summary>
    public required int Port { get; set; }

    /// <summary>Virtual host. Default: /.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>Login username.</summary>
    public required string Username { get; set; }

    /// <summary>Login password.</summary>
    public required string Password { get; set; }

    /// <summary>Prefix applied to all exchange and queue names.</summary>
    public string ExntityPrefix { get; set; } = string.Empty;

    /// <summary>Whether queues and exchanges survive broker restart.</summary>
    public bool Durable { get; set; } = true;

    /// <summary>Whether to declare a dead-letter queue for each endpoint.</summary>
    public bool EnableDeadLetter { get; set; } = true;

    /// <summary>Max concurrent messages processed per consumer.</summary>
    public ushort PrefetchCount { get; set; } = 10;
}
