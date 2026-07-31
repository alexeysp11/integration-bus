using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Represents the immutable asynchronous message contract dispatched over Kafka to trigger account balance adjustments.
/// </summary>
public sealed record TopUpAccountBalance
{
    /// <summary>
    /// Gets the unique distributed correlation identifier assigned to track this specific technical execution flow.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the specific database target account primary identifier destined to receive the financial credit.
    /// </summary>
    public Guid AccountId { get; init; }

    /// <summary>
    /// Gets the precise financial monetary value to be appended onto the asset ledger.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the string identifier of the transactional currency code tracking the underlying asset.
    /// </summary>
    public Currency Currency { get; init; } = Currency.None;

    /// <summary>
    /// Gets the explicit coordinated universal timestamp marking when the transaction boundary was established by the API.
    /// </summary>
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
}
