namespace IntegrationBus.CoreLedger.Contracts.Messages.Events;

/// <summary>
/// Event indicating a constraints or infrastructure failure while writing the journal record.
/// </summary>
public sealed record WriteLedgerRecordFailed
{
    /// <summary>
    /// Gets the correlated tracking identifier for the saga instance.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the infrastructure error summary.
    /// </summary>
    public string Reason { get; init; } = string.Empty;
}
