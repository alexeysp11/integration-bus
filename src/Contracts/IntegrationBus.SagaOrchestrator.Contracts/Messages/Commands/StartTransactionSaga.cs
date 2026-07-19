using IntegrationBus.Contracts.Enums;

namespace IntegrationBus.SagaOrchestrator.Contracts.Messages.Commands;

/// <summary>
/// Command dispatched by the API layer to initiate the stateful distributed transaction saga machine.
/// </summary>
public sealed record StartTransactionSaga
{
    /// <summary>
    /// Gets the unique idempotent identifier for the transaction execution scope.
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// Gets the source account identifier to debit funds from.
    /// </summary>
    public Guid SourceAccountId { get; init; }

    /// <summary>
    /// Gets the target account identifier to credit funds to.
    /// </summary>
    public Guid TargetAccountId { get; init; }

    /// <summary>
    /// Gets the exact financial amount to transfer across services.
    /// </summary>
    public decimal Amount { get; init; }

    /// <summary>
    /// Gets the currency type under which the record is registered.
    /// </summary>
    public Currency Currency { get; init; }
}
