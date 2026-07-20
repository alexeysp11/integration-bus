namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event indicating that the requested funds have been successfully locked.
/// </summary>
public sealed record HoldAccountBalancePassed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the timestamp when the funds hold was applied.
    /// </summary>
    public DateTime HeldAt { get; init; }
}
