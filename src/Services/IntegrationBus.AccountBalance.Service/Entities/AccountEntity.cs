using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.AccountBalance.Service.Entities;

/// <summary>
/// Represents the root aggregate identity reference for a financial account within the event-sourced ecosystem.
/// </summary>
public sealed class AccountEntity
{
    /// <summary>
    /// Gets or sets the primary unique identifier for the target financial account root.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets the string identifier of the transactional currency code tracking the underlying asset.
    /// </summary>
    public Currency Currency { get; init; } = Currency.None;

    /// <summary>
    /// Gets or sets the temporal timestamp tracking exactly when this specific entity aggregate context was initialized.
    /// </summary>
    public DateTime CreatedAt { get; set; }
}
