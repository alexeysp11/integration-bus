namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that the financial compensation sequence failed due to underlying storage or state exceptions.
/// </summary>
public sealed record ReleaseAccountBalanceFailed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the functional or infrastructure error summary detailing the release failure.
    /// </summary>
    public string Reason { get; init; } = string.Empty;

    /// <summary>
    /// Gets the exact timestamp tracking when the release failure event occurred.
    /// </summary>
    public DateTime FailedAtUtc { get; init; } = DateTime.UtcNow;
}
