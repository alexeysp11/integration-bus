using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.Contracts.Http;

/// <summary>
/// Exposes the current execution state of a distributed transaction.
/// </summary>
public sealed record TransactionStatusResponse
{
    /// <summary>
    /// Gets the unique tracking identifier of the transaction.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the current state machine status value.
    /// </summary>
    public TransactionStatus Status { get; init; } = TransactionStatus.None;

    /// <summary>
    /// Gets the technical or business failure reason if the transaction failed.
    /// </summary>
    public string? FailureReason { get; init; }

    /// <summary>
    /// Gets the timestamp of the last status alteration.
    /// </summary>
    public DateTime UpdatedAt { get; init; }
}
