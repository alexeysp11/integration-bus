namespace IntegrationBus.AccountBalance.Service.Entities;

public sealed class AccountHoldEntity
{
    public long Id { get; set; }
    public Guid TransactionId { get; set; }
    public Guid AccountId { get; set; }
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}
