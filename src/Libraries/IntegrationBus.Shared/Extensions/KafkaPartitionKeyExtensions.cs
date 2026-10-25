using MassTransit;

namespace IntegrationBus.Shared.Extensions;

/// <summary>
/// Registers MassTransit Kafka Rider producers that stamp every outbound record with an explicit partition key.
/// </summary>
/// <remarks>
/// Routing rules and rationale: <c>docs/scaling/kafka-partition-keys.md</c> (relative to the repository root).
/// </remarks>
public static class KafkaPartitionKeyExtensions
{
    /// <summary>
    /// Registers an <see cref="ITopicProducer{TValue}"/> for <typeparamref name="T"/> whose records are keyed by the
    /// identifier returned from <paramref name="keySelector"/>.
    /// </summary>
    /// <typeparam name="T">The message contract produced onto the topic.</typeparam>
    /// <param name="rider">The rider registration configurator that owns the producer.</param>
    /// <param name="topicName">The destination Kafka topic.</param>
    /// <param name="keySelector">Selects the partition key identifier (for example an account or transaction id) from the message.</param>
    public static void AddPartitionKeyedProducer<T>(
        this IRiderRegistrationConfigurator rider,
        string topicName,
        Func<T, Guid> keySelector)
        where T : class
    {
        rider.AddProducer<string, T>(topicName, context => ToPartitionKey(keySelector(context.Message)));
    }

    /// <summary>
    /// Formats an identifier into the canonical string representation used as the Kafka record key.
    /// </summary>
    /// <param name="identifier">The account or transaction identifier.</param>
    /// <returns>The lowercase hyphenated identifier, stable across every producer in the platform.</returns>
    public static string ToPartitionKey(Guid identifier) => identifier.ToString("D");
}
