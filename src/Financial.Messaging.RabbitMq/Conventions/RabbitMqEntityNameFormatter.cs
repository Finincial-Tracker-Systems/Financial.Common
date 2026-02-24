using System.Text.Json;

namespace Financial.Messaging.RabbitMq.Conventions;

internal sealed class RabbitMqEntityNameFormatter
{
    private readonly string _prefix;

    public RabbitMqEntityNameFormatter(string prefix)
    {
        _prefix = prefix;
    }

    public string FormatExchangeName<TMessage>() where TMessage : class
    {
        var name = typeof(TMessage).Name;
        var formattedName = Format(name);

        return formattedName;
    }

    public string FormatQueueName<THandler>(string suffixToRemove = "Handler") where THandler : class
    {
        var name = typeof(THandler).Name;
        
        if (name.EndsWith(suffixToRemove))
        {
            name = name.Substring(0, name.Length - suffixToRemove.Length);
        }

        return Format(name);
    }

    public string FormatDeadLetterQueueName(string queueName)
    {
        var deadLetterQueueName = $"{queueName}.error";

        return deadLetterQueueName;
    }

    private string Format(string typeName)
    {
        var kebabName = JsonNamingPolicy.KebabCaseLower.ConvertName(typeName);
        var kebabNameWithPrefix = string.IsNullOrEmpty(_prefix) ? kebabName : $"{_prefix}.{kebabName}";

        return kebabNameWithPrefix;
    }
}
