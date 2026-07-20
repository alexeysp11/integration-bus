namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event confirming that the compensating balance release operation concluded.
/// </summary>
public sealed record ReleaseAccountBalancePassed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }
}
