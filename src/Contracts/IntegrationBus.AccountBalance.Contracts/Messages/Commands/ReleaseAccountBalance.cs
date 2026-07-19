namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Compensating command to reverse previous asset reservations.
/// </summary>
public sealed record ReleaseAccountBalance
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the account identifier where the balance must be unlocked.
    /// </summary>
    public Guid AccountId { get; init; }

    /// <summary>
    /// Gets the exact financial amount to be released.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the contextual justification explaining why compensation was triggered.
    /// </summary>
    public string Reason { get; init; } = string.Empty;
}
