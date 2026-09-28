using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace IntegrationBus.Shared.Security;

/// <summary>
/// Writes masked field payloads directly to Kafka via a dedicated raw <see cref="IProducer{TKey, TValue}"/>,
/// bypassing MassTransit's producer registry so the masking pipeline can target any source topic at runtime
/// without requiring a statically declared MassTransit producer for every <c>{topic}.security</c> destination.
/// </summary>
public sealed class KafkaSecurityTopicPublisher : ISecurityTopicPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaSecurityTopicPublisher> _logger;

    public KafkaSecurityTopicPublisher(string bootstrapServers, ILogger<KafkaSecurityTopicPublisher> logger)
    {
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();
    }

    /// <inheritdoc />
    public async Task PublishMaskedAsync(
        string sourceTopic,
        string key,
        IReadOnlyDictionary<string, string> maskedFields,
        CancellationToken cancellationToken)
    {
        string securityTopic = $"{sourceTopic}.security";
        string payload = JsonSerializer.Serialize(maskedFields);

        DeliveryResult<string, string> result = await _producer.ProduceAsync(
            securityTopic,
            new Message<string, string> { Key = key, Value = payload },
            cancellationToken);

        _logger.LogDebug(
            "Published masked security shadow record to {SecurityTopic} at partition {Partition}, offset {Offset}",
            securityTopic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer.Dispose();
}
