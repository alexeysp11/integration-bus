namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Represents the asynchronous integration event signaling that an account balance 
/// replenishment operation failed due to business constraint violations or internal infrastructure faults.
/// </summary>
public sealed record TopUpAccountBalanceFailed
{
    /// <summary>
    /// Gets or sets the unique distributed tracking identifier assigned to correlate 
    /// this failed processing step across the system.
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Gets or sets the primary identifier of the database account entity 
    /// where the replenishment operation was attempted.
    /// </summary>
    public Guid AccountId { get; set; }

    /// <summary>
    /// Gets or sets the descriptive error message outlining the root cause 
    /// of the execution pipeline failure.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// Gets or sets the explicit coordinated universal timestamp marking exactly 
    /// when the processing pipeline aborted and generated the error state.
    /// </summary>
    public DateTime FailedAtUtc { get; set; }
}