namespace IntegrationBus.CoreLedger.Service.Models;

public sealed record PublishLedgerCommittedArguments
{
    public Guid TransactionId { get; init; }
    public decimal Amount { get; init; }
    public int Currency { get; init; }
}
