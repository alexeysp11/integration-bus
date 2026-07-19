namespace IntegrationBus.Contracts.Http;

/// <summary>
/// External client request to initialize a new distributed transaction.
/// </summary>
public sealed record StartTransactionRequest
{
    /// <summary>
    /// Gets the unique idempotent identifier for the transaction.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the source account identifier to debit funds from.
    /// </summary>
    public Guid SourceAccountId { get; init; }

    /// <summary>
    /// Gets the target account identifier to credit funds to.
    /// </summary>
    public Guid TargetAccountId { get; init; }

    /// <summary>
    /// Gets the exact financial volume to transfer.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the ISO 4217 currency code.
    /// </summary>
    public string Currency { get; init; } = string.Empty;
}
