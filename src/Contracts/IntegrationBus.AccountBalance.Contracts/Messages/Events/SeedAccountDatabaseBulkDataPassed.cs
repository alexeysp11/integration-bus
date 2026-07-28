namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event produced when the bulk account seeding operation completes successfully.
/// </summary>
public sealed record SeedAccountDatabaseBulkDataPassed
{
    /// <summary>
    /// Gets the total number of randomized account records successfully written to the persistent store.
    /// </summary>
    public required int SeededQuantity { get; init; }
}
