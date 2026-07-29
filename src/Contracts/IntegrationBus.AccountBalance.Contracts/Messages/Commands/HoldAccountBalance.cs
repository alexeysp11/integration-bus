using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Command to reserve specific transaction funds.
/// </summary>
public sealed record HoldAccountBalance
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public required Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the source account identifier where funds must be locked.
    /// </summary>
    public required Guid AccountFromId { get; init; }

    /// <summary>
    /// Gets the target account identifier where funds must be locked.
    /// </summary>
    public required Guid AccountToId { get; init; }

    /// <summary>
    /// Gets the strict transactional currency code tracking the underlying liquid asset allocation type.
    /// </summary>
    public required Currency Currency { get; init; } = Currency.None;

    /// <summary>
    /// Gets the exact financial amount to allocate.
    /// </summary>
    public required decimal Amount { get; init; }
}
