namespace IntegrationBus.Compliance.Contracts.Messages.Commands;

/// <summary>
/// Command to validate declarative transaction limits and regulatory rules.
/// </summary>
public sealed record CheckComplianceLimits
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the source account identifier initiating the request.
    /// </summary>
    public Guid SourceAccountId { get; init; }

    /// <summary>
    /// Gets the destination account identifier receiving the transfer.
    /// </summary>
    public Guid TargetAccountId { get; init; }

    /// <summary>
    /// Gets the transfer volume required for compliance verification.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the currency type used to evaluate boundary constraints.
    /// </summary>
    public string Currency { get; init; } = string.Empty;
}
