namespace IntegrationBus.AccountBalance.Contracts.Messages.Commands;

/// <summary>
/// Domain infrastructure request command dispatched into Kafka to initiate a background high speed bulk seeding operation.
/// </summary>
public sealed record SeedAccountDatabaseBulkData
{
    /// <summary>
    /// Gets the absolute total number of valid randomized account entity records required to be populated into the target repository.
    /// </summary>
    public int RecordQuantity { get; init; }
}
