namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event produced when the bulk account seeding operation fails due to an unhandled execution exception.
/// </summary>
public sealed record SeedAccountDatabaseBulkDataFailed
{
    /// <summary>
    /// Gets the total number of randomized account records successfully written to the persistent store.
    /// </summary>
    public required int SeededQuantity { get; init; }

    /// <summary>
    /// Gets the descriptive system failure message detailing the root cause of the seeding abortion.
    /// </summary>
    public string FailureReason { get; init; } = string.Empty;
}
