namespace IntegrationBus.CoreLedger.Service.Entities;

public sealed record LedgerEntryEntity
{
    public required long Id { get; set; }
    public required Guid TransactionId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
