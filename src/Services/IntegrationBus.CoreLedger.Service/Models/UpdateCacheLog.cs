namespace IntegrationBus.CoreLedger.Service.Models;

public sealed record UpdateCacheLog
{
    public Guid TransactionId { get; init; }
}
