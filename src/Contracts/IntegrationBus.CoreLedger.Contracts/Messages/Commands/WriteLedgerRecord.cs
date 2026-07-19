namespace IntegrationBus.CoreLedger.Contracts.Messages.Commands;

/// <summary>
/// Command to commit the immutable final financial record.
/// </summary>
public sealed record WriteLedgerRecord
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the verified source account identifier.
    /// </summary>
    public Guid SourceAccountId { get; init; }

    /// <summary>
    /// Gets the verified target account identifier.
    /// </summary>
    public Guid TargetAccountId { get; init; }

    /// <summary>
    /// Gets the finalized audit amount to be written.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the currency type under which the record is registered.
    /// </summary>
    public string Currency { get; init; } = string.Empty;
}
