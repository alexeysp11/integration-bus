namespace IntegrationBus.CoreLedger.Service.Models;

public sealed record WriteAuditTrailArguments
{
    public Guid TransactionId { get; init; }
    public Guid SourceAccountId { get; init; }
    public Guid TargetAccountId { get; init; }
    public decimal Amount { get; init; }
    public int Currency { get; init; }
}
