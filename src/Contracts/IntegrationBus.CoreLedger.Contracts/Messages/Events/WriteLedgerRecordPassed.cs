namespace IntegrationBus.CoreLedger.Contracts.Messages.Events;

/// <summary>
/// Event confirming that the transactional entry has been appended to the ledger.
/// </summary>
public sealed record WriteLedgerRecordPassed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the unique generated log entry sequential identifier.
    /// </summary>
    public long EntryId { get; init; }

    /// <summary>
    /// Gets the timestamp when the journal entry was saved.
    /// </summary>
    public DateTime CreatedAt { get; init; }
}
