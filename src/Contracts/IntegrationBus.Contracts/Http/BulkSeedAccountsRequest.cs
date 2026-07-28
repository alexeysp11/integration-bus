namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Represents the inbound payload required to execute an environment-gated bulk data seeding operation.
/// </summary>
public sealed record BulkSeedAccountsRequest
{
    /// <summary>
    /// Gets the total number of test account entities to generate and seed into the database.
    /// </summary>
    public int Count { get; init; } = 100000;
}
