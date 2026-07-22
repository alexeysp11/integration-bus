namespace IntegrationBus.CoreLedger.Service.Models;

public sealed record WriteAuditTrailLog
{
    public Guid TransactionId { get; init; }
    public long LedgerEntryId { get; init; }
}
