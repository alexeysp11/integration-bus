namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Represents the outbound structured HTTP API response confirming the ingestion of the replenishment command.
/// </summary>
public sealed record TopUpAccountResponse
{
    /// <summary>
    /// Gets the globally unique tracking correlation identifier generated for the transaction processing audit trail.
    /// </summary>
    /// <example>9f5b61e2-411a-4c22-990a-c8e6b12a5db3</example>
    public Guid TrackingTransactionId { get; init; }

    /// <summary>
    /// Gets the human-readable operational description indicating the current ingestion state of the request.
    /// </summary>
    /// <example>Top-up request accepted and is being processed asynchronously.</example>
    public string? Message { get; init; }
}
