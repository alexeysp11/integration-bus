namespace IntegrationBus.Shared.Security;

/// <summary>
/// Mirrors masked (HMAC-hashed) copies of confidential message fields into dedicated Kafka security topics,
/// independent of the MassTransit producer/topic-endpoint topology used for ordinary business messages.
/// </summary>
public interface ISecurityTopicPublisher
{
    /// <summary>
    /// Publishes the masked field map for a single consumed message onto <c>{sourceTopic}.security</c>.
    /// </summary>
    /// <param name="sourceTopic">The original Kafka topic the raw (unmasked) message was consumed from.</param>
    /// <param name="key">Kafka partition key for the masked record; typically the transaction id.</param>
    /// <param name="maskedFields">Property name to masked-value map produced by <see cref="IHmacMaskingService"/>.</param>
    /// <param name="cancellationToken">Propagates cancellation from the originating consume context.</param>
    Task PublishMaskedAsync(
        string sourceTopic,
        string key,
        IReadOnlyDictionary<string, string> maskedFields,
        CancellationToken cancellationToken);
}
