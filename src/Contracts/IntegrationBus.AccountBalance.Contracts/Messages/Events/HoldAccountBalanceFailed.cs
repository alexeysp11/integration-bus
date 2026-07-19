namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that funds allocation failed.
/// </summary>
public sealed record HoldAccountBalanceFailed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the business or technical failure details.
    /// </summary>
    public string Reason { get; init; } = string.Empty;
}
