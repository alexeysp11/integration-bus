using IntegrationBus.AccountBalance.Service.Enums;

namespace IntegrationBus.AccountBalance.Service.Entities;

/// <summary>
/// Represents an immutable, append-only financial ledger record tracking a specific credit or debit transaction event.
/// </summary>
public sealed class AccountJournalEntryEntity
{
    /// <summary>
    /// Gets or sets the auto-incrementing transaction entry sequence identifier.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// Gets or sets the primary source financial account identity that owns this sequence stream tracking.
    /// </summary>
    public Guid SourceAccountId { get; set; }

    /// <summary>
    /// Gets or sets the optional target or counterparty financial account destination identity context.
    /// </summary>
    public Guid? TargetAccountId { get; set; }

    /// <summary>
    /// Gets or sets the strict sequential atomic version offset number used for transaction positioning and concurrency validation.
    /// </summary>
    public long SequenceNumber { get; set; }

    /// <summary>
    /// Gets or sets the exact signed transaction value magnitude (positive for deposits/credits, negative for withdrawals/debits).
    /// </summary>
    public decimal AmountDelta { get; set; }

    /// <summary>
    /// Gets or sets the lifecycle status behavior pattern applied to this specific event log entry.
    /// </summary>
    public JournalEntryType EntryType { get; set; }

    /// <summary>
    /// Gets or sets the operational transaction reference context mapping this event log entry to external tracking systems.
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Gets or sets the exact temporal point tracking when this immutable ledger record was persisted into the stream log.
    /// </summary>
    public DateTime TimestampUtc { get; set; }
}
