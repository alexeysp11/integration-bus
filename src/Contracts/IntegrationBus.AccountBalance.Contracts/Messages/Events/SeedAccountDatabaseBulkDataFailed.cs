namespace IntegrationBus.AccountBalance.Contracts.Messages.Events;

/// <summary>
/// Event produced when the bulk account seeding operation fails due to an unhandled execution exception.
/// </summary>
public sealed record SeedAccountDatabaseBulkDataFailed
{
    /// <summary>
    /// Gets the descriptive system failure message detailing the root cause of the seeding abortion.
    /// </summary>
    public string FailureReason { get; init; } = string.Empty;
}
