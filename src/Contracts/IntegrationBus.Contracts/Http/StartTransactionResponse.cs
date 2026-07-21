namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Represents the asynchronous acceptance receipt delivered back to the calling client.
/// </summary>
public sealed record StartTransactionResponse
{
    /// <summary>
    /// The authoritative unique identifier associated with the newly initialized stateful transaction.
    /// </summary>
    public required Guid TransactionId { get; init; }

    /// <summary>
    /// The near-real-time ingestion state of the distributed transaction. Always returns 'Processing' initially.
    /// </summary>
    public required string Status { get; init; }

    /// <summary>
    /// Descriptive infrastructural notification message validating successful broker queuing boundaries.
    /// </summary>
    public string? Message { get; init; }
}
