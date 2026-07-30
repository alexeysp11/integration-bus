namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event confirming that the asset hold has been successfully finalized and double-entry ledger streams are updated.
/// </summary>
public sealed record ConfirmAccountBalancePassed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the timestamp when the confirmation journal entries were officially persisted.
    /// </summary>
    public DateTime ConfirmedAtUtc { get; init; }
}
