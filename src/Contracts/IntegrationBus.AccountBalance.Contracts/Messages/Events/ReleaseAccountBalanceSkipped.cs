namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that the balance release operation was safely skipped because no active hold record existed.
/// </summary>
public sealed record ReleaseAccountBalanceSkipped
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the timestamp tracking when the idempotency skip condition was evaluated.
    /// </summary>
    public DateTime SkippedAtUtc { get; init; } = DateTime.UtcNow;
}
