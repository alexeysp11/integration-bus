namespace IntegrationBus.AccountBalance.Service.Entities;

public sealed class AccountEntity
{
    public Guid Id { get; set; }
    public decimal Balance { get; set; }
    public DateTime UpdatedAt { get; set; }
}
