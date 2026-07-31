namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that the financial confirmation sequence failed due to concurrency or state exceptions.
/// </summary>
public sealed record ConfirmAccountBalanceFailed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the functional or infrastructure error summary detailing the confirmation stoppage.
    /// </summary>
    public string Reason { get; init; } = string.Empty;

    /// <summary>
    /// Gets the exact timestamp tracking when the confirmation failure event occurred.
    /// </summary>
    public DateTime FailedAtUtc { get; init; } = DateTime.UtcNow;
}
