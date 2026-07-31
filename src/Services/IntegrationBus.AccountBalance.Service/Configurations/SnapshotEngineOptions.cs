namespace IntegrationBus.AccountBalance.Service.Configurations;

/// <summary>
/// Represents the configuration matrix thresholds for the background event-sourced snapshot engine.
/// </summary>
public sealed record SnapshotEngineOptions
{
    /// <summary>
    /// Gets or sets the sequential milestone delta gap that triggers an absolute checkpoint evaluation.
    /// </summary>
    public int SequenceThreshold { get; init; } = 50;

    /// <summary>
    /// Gets or sets the temporal frequency delay interval executed between background database scanning passes.
    /// </summary>
    public TimeSpan ExecutionInterval { get; init; } = TimeSpan.FromMinutes(5);
}
