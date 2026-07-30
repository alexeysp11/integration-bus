namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Command destined for the Accounting service to finalize the frozen asset reservation hold.
/// </summary>
public sealed record ConfirmAccountBalance
{
    /// <summary>
    /// Gets the unique distributed correlation identifier assigned to track this specific technical execution flow.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the coordinated universal timestamp marking when the confirmation request was emitted.
    /// </summary>
    public DateTime TimestampUtc { get; init; } = DateTime.UtcNow;
}
