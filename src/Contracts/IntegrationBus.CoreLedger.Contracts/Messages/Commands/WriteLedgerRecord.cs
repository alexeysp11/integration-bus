using IntegrationBus.Contracts.Enums;
using IntegrationBus.Contracts.Security;

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
    [SensitiveData]
    public Guid SourceAccountId { get; init; }

    /// <summary>
    /// Gets the verified target account identifier.
    /// </summary>
    [SensitiveData]
    public Guid TargetAccountId { get; init; }

    /// <summary>
    /// Gets the finalized audit amount to be written.
    /// </summary>
    [SensitiveData]
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the currency type under which the record is registered.
    /// </summary>
    public Currency Currency { get; init; }
}
