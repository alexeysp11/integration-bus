namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Represents the asynchronous integration event signaling that an individual account 
/// balance replenishment operation was successfully validated and committed to the ledger.
/// </summary>
public sealed record TopUpAccountBalancePassed
{
    /// <summary>
    /// Gets or sets the unique distributed tracking identifier assigned to correlate 
    /// this processing lifecycle step across the system.
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Gets or sets the primary identifier of the specific database account entity 
    /// that received the financial credit.
    /// </summary>
    public Guid AccountId { get; set; }

    /// <summary>
    /// Gets or sets the precise monetary value that was successfully appended 
    /// to the account asset ledger.
    /// </summary>
    public decimal Amount { get; set; }

    /// <summary>
    /// Gets or sets the explicit coordinated universal timestamp marking exactly 
    /// when the ledger record transaction write boundary was finalized.
    /// </summary>
    public DateTime CompletedAtUtc { get; set; }
}