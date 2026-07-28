namespace IntegrationBus.AccountBalance.Service.Entities;

/// <summary>
/// Represents the persistent entity model mapped to a user or system financial account containing real time balance value state trackers.
/// </summary>
public sealed class AccountEntity
{
    /// <summary>
    /// Gets or sets the primary unique identifier for the target account record ledger.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets or sets the exact current liquid asset capacity allocation stored within the explicit financial account context.
    /// </summary>
    public decimal Balance { get; set; }

    /// <summary>
    /// Gets or sets the temporal checkpoint tracking when the entity instance balance schema values were last modified.
    /// </summary>
    public DateTime UpdatedAt { get; set; }
}
