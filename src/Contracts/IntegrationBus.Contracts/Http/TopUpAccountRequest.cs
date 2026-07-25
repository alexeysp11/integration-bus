using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Represents the inbound HTTP contract payload required to execute an account balance replenishment operation.
/// </summary>
public sealed record TopUpAccountRequest
{
    /// <summary>
    /// Gets the specific financial asset allocation quantity to be credited to the account.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the currency code identifying the asset type.
    /// </summary>
    public Currency Currency { get; init; } = Currency.None;
}
