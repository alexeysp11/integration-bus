namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Command to reserve specific transaction funds.
/// </summary>
public sealed record HoldAccountBalance
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the source account identifier where funds must be locked.
    /// </summary>
    public Guid AccountId { get; init; }

    /// <summary>
    /// Gets the exact financial amount to allocate.
    /// </summary>
    public decimal Amount { get; init; }
}
