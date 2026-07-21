namespace IntegrationBus.Compliance.Contracts.Messages.Events;

/// <summary>
/// Event confirming that the transaction satisfies all compliance policies.
/// </summary>
public sealed record CheckComplianceLimitsPassed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    // TODO: add xml comment.
    public DateTime VerifiedAt { get; init; } = DateTime.UtcNow;
}
