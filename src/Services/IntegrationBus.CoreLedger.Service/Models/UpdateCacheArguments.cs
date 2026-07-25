namespace IntegrationBus.CoreLedger.Service.Models;

public sealed record UpdateCacheArguments
{
    public Guid TransactionId { get; init; }
    public decimal Amount { get; init; }
}
