namespace IntegrationBus.Compliance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that the transaction breached legal or anti-fraud limits.
/// </summary>
public sealed record CheckComplianceLimitsFailed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the descriptive violation explanation.
    /// </summary>
    public string Reason { get; init; } = string.Empty;
}
