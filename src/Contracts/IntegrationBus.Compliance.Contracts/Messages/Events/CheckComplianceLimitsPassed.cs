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

    /// <summary>
    /// Gets the strict coordinated universal temporal timestamp tracking exactly when the corporate compliance verification sequence succeeded.
    /// </summary>
    public DateTime VerifiedAt { get; init; } = DateTime.UtcNow;
}
